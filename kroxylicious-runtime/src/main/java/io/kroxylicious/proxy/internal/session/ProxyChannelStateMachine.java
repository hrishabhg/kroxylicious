/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.router.AggregationContext;
import io.kroxylicious.proxy.internal.router.Router;
import io.kroxylicious.proxy.internal.router.TopicRouter;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.service.ServiceEndpoint;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages multiple backend cluster connections for a single client session.
 *
 * <p>This class serves as the orchestrator for backend connections, providing:</p>
 * <ul>
 *   <li>Connection initiation (single cluster or multi-cluster)</li>
 *   <li>Cluster registration and lookup</li>
 *   <li>Connection lifecycle management</li>
 *   <li>Request forwarding and routing</li>
 *   <li>Backpressure coordination across clusters</li>
 *   <li>Response routing back to the client</li>
 * </ul>
 *
 * <p>For single-cluster deployments, this manager contains exactly one backend.
 * For multi-cluster, it contains one backend per configured cluster.</p>
 *
 * <h2>Architecture</h2>
 * <pre>
 *   ClientSessionStateMachine
 *           │
 *           ▼
 *   ProxyChannelStateMachine  ◀── NetFilter initiates connections here
 *           │
 *     ┌─────┼─────┐
 *     ▼     ▼     ▼
 *   BSM₁  BSM₂  BSM₃  (BackendStateMachines)
 *     │     │     │
 *     ▼     ▼     ▼
 *   Cluster₁ Cluster₂ Cluster₃
 * </pre>
 */
public class ProxyChannelStateMachine {
    private static final String DUPLICATE_INITIATE_CONNECT_ERROR = "onInitiateConnect called more than once";
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyChannelStateMachine.class);

    private final String sessionId;
    private final String virtualClusterName;
    private final KafkaProxyFrontendHandler frontendHandler;
    private final int socketFrameMaxSizeBytes;
    private final boolean logNetwork;
    private final boolean logFrames;

    // Cluster backends
    private final Map<ServiceEndpoint, BackendStateMachine> backends = new ConcurrentHashMap<>();

    // Target clusters by cluster ID
    private final Map<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();

    // Correlation counter for aggregating responses
    private final Map<Integer, AggregationContext<?>> aggregationCorrelationManager = new ConcurrentHashMap<>();

    private volatile ServiceEndpoint defaultTarget;
    private final EndpointBinding endpointBinding;

    private @Nullable Channel inboundChannel;

    // Filters applied to this session
    private @Nullable List<FilterAndInvoker> filters;

    private final Router router;

    // Connection state
    private volatile boolean anyConnected = false;

    public ProxyChannelStateMachine(
                                    String virtualClusterName,
                                    EndpointBinding endpointBinding,
                                    int socketFrameMaxSizeBytes,
                                    boolean logNetwork,
                                    boolean logFrames) {
        this.virtualClusterName = Objects.requireNonNull(virtualClusterName);
        this.socketFrameMaxSizeBytes = socketFrameMaxSizeBytes;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.endpointBinding = endpointBinding;
        this.router = new TopicRouter();
    }

    // ==================== Connection Initiation (Orchestration Entry Points) ====================

    /**
     * Initiate connections to multiple clusters.
     *
     * <p>This is the main entry point for multi-cluster connections,
     * called by NetFilter via ClientSessionStateMachine.</p>
     *
     * @param serviceEndpoints map of clusterId to target cluster configuration
     * @param filters protocol filters to apply
     * @param inboundChannel client channel
     * @return future that completes when ALL clusters are connected
     */
    public CompletableFuture<Void> initiateMultiClusterConnection(
                                                                  List<ServiceEndpoint> serviceEndpoints,
                                                                  List<FilterAndInvoker> filters,
                                                                  Channel inboundChannel) {

        this.filters = filters;
        this.inboundChannel = inboundChannel;

        LOGGER.info("{}: Initiating multi-cluster connection to {} clusters with {} filters",
                sessionId, serviceEndpoints.size(), filters.size());

        // Register all clusters
        serviceEndpoints.forEach(this::addServiceEndpoint);

        // Connect all concurrently
        return connectAll(inboundChannel);
    }

    /**
     * Get the filters applied to this session.
     */
    @Nullable
    public List<FilterAndInvoker> filters() {
        return filters;
    }

    // ==================== Cluster Registration ====================

    /**
     * Register a cluster with explicit node ID offset.
     *
     * @param serviceEndpoint service endpoint for this cluster
     * @return the created backend state machine
     */
    public BackendStateMachine addServiceEndpoint(ServiceEndpoint serviceEndpoint) {
        if (backends.containsKey(serviceEndpoint)) {
            throw new IllegalArgumentException("Cluster already registered: " + serviceEndpoint.getHostPort());
        }

        if (serviceEndpoints.containsKey(serviceEndpoint.targetCluster().name())) {
            // throw exception that only one service endpoint per cluster is allowed
            throw new IllegalArgumentException("Only one service endpoint per cluster is allowed: " + serviceEndpoint.getHostPort());
        }

        Counter connectionCounter = Metrics.proxyToServerConnectionCounter(virtualClusterName, null).withTags();
        Counter errorCounter = Metrics.proxyToServerErrorCounter(virtualClusterName, null).withTags();
        Timer backpressureTimer = Metrics.serverToProxyBackpressureTimer(virtualClusterName, null).withTags();

        BackendStateMachine backend = new BackendStateMachine(
                serviceEndpoint,
                this,
                socketFrameMaxSizeBytes,
                logNetwork,
                logFrames,
                connectionCounter,
                errorCounter,
                backpressureTimer);

        backends.put(serviceEndpoint, backend);
        serviceEndpoints.put(serviceEndpoint.targetCluster().name(), serviceEndpoint);

        return backend;
    }

    // ==================== Accessors ====================

    public String sessionId() {
        return sessionId;
    }

    public Set<String> clusterIds() {
        return serviceEndpoints.keySet();
    }

    public Collection<BackendStateMachine> allBackends() {
        return Collections.unmodifiableCollection(backends.values());
    }

    @Nullable
    public BackendStateMachine getBackend(String clusterId) {
        ServiceEndpoint serviceEndpoint = serviceEndpoints.get(clusterId);
        return backends.get(serviceEndpoint);
    }

    public boolean isMultiCluster() {
        return backends.size() > 1;
    }

    public boolean isAnyConnected() {
        return anyConnected;
    }

    public boolean areAllConnected() {
        return backends.values().stream().allMatch(BackendStateMachine::isConnected);
    }

    /**
     * Get all connected backends.
     */
    public List<BackendStateMachine> connectedBackends() {
        return backends.values().stream()
                .filter(BackendStateMachine::isConnected)
                .collect(Collectors.toList());
    }

    // ==================== Connection Management (Internal) ====================

    /**
     * Connect to all registered clusters concurrently.
     *
     * @param inboundChannel client channel
     * @return future that completes when ALL clusters are connected
     */
    public CompletableFuture<Void> connectAll(Channel inboundChannel) {

        List<CompletableFuture<BackendStateMachine>> futures = new ArrayList<>();

        for (BackendStateMachine backend : backends.values()) {
            futures.add(backend.connect(inboundChannel));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    // ==================== Request Forwarding ====================

    /**
     * Forward a message to the primary cluster.
     */
    public void forwardToServer(Object msg) {
        List<ServiceEndpoint> msgTargets;
        if (msg instanceof Frame frame) {
            msgTargets = endpointBinding.upstreamServiceEndpoints(ApiKeys.forId(frame.apiKeyId()));
            if (!aggregationCorrelationManager.containsKey(frame.correlationId())) {
                aggregationCorrelationManager.put(frame.correlationId(), new AggregationContext<>(msgTargets.size()));
            }
        }
        else {
            // todo: do we need to handle non-Frame messages here?
            throw new IllegalArgumentException("Invalid message type: " + msg.getClass());
        }

        // todo: check all backends are connected. we can implement re-connect logic.

        backends.values().forEach(backend -> {
            if (backend == null) { // this should never happen
                throw new IllegalArgumentException("Unknown cluster for message: " + msg);
            }

            if (!backend.isConnected()) {
                throw new IllegalStateException("backend not connected for cluster: " + backend.clusterId());
            }

            backend.forwardToServer(msg);
        });
    }

    /**
     * Forward a message to a specific cluster.
     */
    public void forwardToServer(String clusterId, Object msg) {
        BackendStateMachine backend = getBackend(clusterId);
        if (backend == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterId);
        }
        if (!backend.isConnected()) {
            throw new IllegalStateException("Cluster not connected: " + clusterId);
        }
        backend.forwardToServer(msg);
    }

    /**
     * Flush pending writes on all connected backends.
     */
    public void flushAll() {
        for (BackendStateMachine backend : backends.values()) {
            if (backend.isConnected()) {
                backend.flushToServer();
            }
        }
    }

    /**
     * Flush pending writes on specific cluster.
     */
    public void flush(String clusterId) {
        BackendStateMachine backend = backends.get(clusterId);
        if (backend != null && backend.isConnected()) {
            backend.flushToServer();
        }
    }

    // ==================== Backpressure Management ====================

    /**
     * Apply backpressure to all backends (client is slow).
     */
    public void applyBackpressureToAll() {
        for (BackendStateMachine backend : backends.values()) {
            if (backend.isConnected()) {
                backend.applyBackpressure();
            }
        }
    }

    /**
     * Relieve backpressure on all backends (client caught up).
     */
    public void relieveBackpressureFromAll() {
        for (BackendStateMachine backend : backends.values()) {
            if (backend.isConnected()) {
                backend.relieveBackpressure();
            }
        }
    }

    // ==================== Callbacks from BackendStateMachine ====================

    void onBackendConnected(BackendStateMachine backend) {
        this.anyConnected = true;
        LOGGER.debug("{}: Backend connected: {}", sessionId, backend.clusterId());
        sessionStateMachine.onBackendConnected(backend);
    }

    void onBackendFailed(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend failed: {} - {}", sessionId, backend.clusterId(), cause.getMessage());
        sessionStateMachine.onBackendFailed(backend, cause);
    }

    void onBackendClosed(BackendStateMachine backend) {
        LOGGER.debug("{}: Backend closed: {}", sessionId, backend.clusterId());

        // Update anyConnected status
        this.anyConnected = backends.values().stream()
                .anyMatch(BackendStateMachine::isConnected);

        sessionStateMachine.onBackendClosed(backend);
    }

    void onBackendError(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend error: {} - {}", sessionId, backend.clusterId(), cause.getMessage());
        sessionStateMachine.onBackendError(backend, cause);
    }

    void onBackendResponse(BackendStateMachine backend, Object msg) {
        // Forward response to session state machine which routes to frontend
        // aggregate responses based on correlation counter
        // todo: to ensure decoded response, the correlation manager should say decodeResponse=true for aggregated requests
        if (msg instanceof DecodedResponseFrame<?> frame) {
            AggregationContext aggContext = aggregationCorrelationManager.get(frame.correlationId());
            aggContext.addResponse(backend.targetCluster(), frame);
            int remaining = aggContext.remainingResponses();
            LOGGER.debug("{}: Received response for correlationId {}. Remaining: {}",
                    sessionId, frame.correlationId(), remaining);
            if (remaining > 0) {
                LOGGER.debug("{}: Waiting for more responses for correlationId {}. Remaining: {}",
                        sessionId, frame.correlationId(), remaining);
                return;
            }

            aggregationCorrelationManager.remove(frame.correlationId());

            LOGGER.debug("{}: All responses received for correlationId {}. Forwarding to session.",
                    sessionId, frame.correlationId());

            ApiMessageAggregator<?> aggregator = router.aggregator(frame.apiKey());
            if (aggregator != null) {
                ApiMessage aggregatedResponse = aggregator.aggregateResponses(aggContext);
                var aggregatedMsg = new DecodedResponseFrame<>(
                        frame.apiVersion(),
                        frame.correlationId(),
                        frame.header(),
                        aggregatedResponse);
                LOGGER.debug("{}: Aggregated response for correlationId {} using {} aggregator.",
                        sessionId, frame.correlationId(), aggregator.getClass().getSimpleName());
                sessionStateMachine.onBackendResponse(backend, aggregatedMsg);
                return;
            }
            else {
                LOGGER.debug("{}: No aggregator found for API key {}. Forwarding last response to session.",
                        sessionId, frame.apiKey());
            }
        }
        else {
            LOGGER.debug("{}: Received non-frame response. Forwarding to session.", sessionId);
            throw new IllegalArgumentException("Received non-frame response");
        }
        sessionStateMachine.onBackendResponse(backend, msg);
    }

    void onBackendReadComplete(BackendStateMachine backend) {
        sessionStateMachine.onBackendReadComplete(backend);
    }

    void onBackendWritable(BackendStateMachine backend) {
        sessionStateMachine.onBackendWritable(backend);
    }

    void onBackendUnwritable(BackendStateMachine backend) {
        sessionStateMachine.onBackendUnwritable(backend);
    }

    // ==================== Cleanup ====================

    /**
     * Close all backend connections.
     */
    public void closeAll() {
        LOGGER.debug("{}: Closing all backends", sessionId);
        for (BackendStateMachine backend : backends.values()) {
            backend.close();
        }
    }

    @Override
    public String toString() {
        return "ProxyChannelStateMachine{" +
                "sessionId='" + sessionId + '\'' +
                ", clusters=" + backends.keySet() +
                ", primaryClusterId='" + defaultTarget + '\'' +
                ", anyConnected=" + anyConnected +
                '}';
    }
}

/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.util.ReferenceCountUtil;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;
import io.kroxylicious.proxy.internal.router.Router;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.internal.util.ActivationToken;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;
import io.kroxylicious.proxy.internal.util.VirtualClusterNode;
import io.kroxylicious.proxy.service.UpstreamEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Startup.STARTING_STATE;

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

    // Connection metrics
    private final Counter clientToProxyErrorCounter;
    private final Counter clientToProxyConnectionCounter;
    private final Counter proxyToServerConnectionCounter;
    private final Counter proxyToServerErrorCounter;
    private final Timer serverToProxyBackpressureMeter;
    private final Timer clientToProxyBackPressureMeter;

    private final ActivationToken clientToProxyConnectionToken;
    private final ActivationToken proxyToServerConnectionToken;

    @VisibleForTesting
    @Nullable
    Timer.Sample clientToProxyBackpressureTimer;

    @VisibleForTesting
    @Nullable
    Timer.Sample serverBackpressureTimer;

    @Nullable
    private String sessionId;

    /**
     * The current state. This can be changed via a call to one of the {@code on*()} methods.
     */
    private ProxyChannelState state = STARTING_STATE;

    /*
     * The netty autoread flag is volatile =>
     * expensive to set in every call to channelRead.
     * So we track autoread states via these non-volatile fields,
     * allowing us to only touch the volatile when it needs to be changed
     */
    @VisibleForTesting
    boolean serverReadsBlocked;
    @VisibleForTesting
    boolean clientReadsBlocked;
    private @Nullable Timer.Sample clientBackpressureTimer;

    /**
     * The frontend handler. Non-null if we got as far as ClientActive.
     */
    @SuppressWarnings({ "java:S2637" })
    private @Nullable KafkaProxyFrontendHandler frontendHandler = null;

    // Cluster backends
    private final Map<UpstreamEndpoint, BackendStateMachine> backends = new ConcurrentHashMap<>();

    // Target clusters by cluster ID
    private final Map<String, UpstreamEndpoint> serviceEndpoints = new ConcurrentHashMap<>();

    // Correlation counter for aggregating responses
    private final Map<Integer, ResponseAggregationContext<?>> aggregationCorrelationManager = new ConcurrentHashMap<>();

    private final String virtualClusterName;
    private final int socketFrameMaxSizeBytes;
    private final boolean logNetwork;
    private final boolean logFrames;

    private volatile UpstreamEndpoint defaultTarget;
    private final EndpointBinding binding;

    private EndpointReconciler reconciler;

    // Connection state
    private volatile boolean anyConnected = false;

    public ProxyChannelStateMachine(EndpointBinding binding, EndpointReconciler reconciler) {
        var virtualCluster = binding.endpointGateway().virtualCluster();
        this.virtualClusterName = Objects.requireNonNull(virtualCluster.getClusterName());
        this.socketFrameMaxSizeBytes = virtualCluster.socketFrameMaxSizeBytes();
        this.logNetwork = virtualCluster.isLogNetwork();
        this.logFrames = virtualCluster.isLogFrames();
        this.binding = binding;
        this.reconciler = reconciler;

        // Initialize metrics
        Integer nodeId = binding.virtualNodeId();
        VirtualClusterNode node = new VirtualClusterNode(virtualClusterName, nodeId);
        // Connection metrics
        clientToProxyConnectionCounter = Metrics.clientToProxyConnectionCounter(virtualClusterName, nodeId).withTags();
        clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter(virtualClusterName, nodeId).withTags();
        proxyToServerConnectionCounter = Metrics.proxyToServerConnectionCounter(virtualClusterName, nodeId).withTags();
        proxyToServerErrorCounter = Metrics.proxyToServerErrorCounter(virtualClusterName, nodeId).withTags();
        serverToProxyBackpressureMeter = Metrics.serverToProxyBackpressureTimer(virtualClusterName, nodeId).withTags();
        clientToProxyBackPressureMeter = Metrics.clientToProxyBackpressureTimer(virtualClusterName, nodeId).withTags();
        clientToProxyConnectionToken = Metrics.clientToProxyConnectionToken(node);
        proxyToServerConnectionToken = Metrics.proxyToServerConnectionToken(node);
    }

    ProxyChannelState state() {
        return state;
    }

    @VisibleForTesting
    void allocateSessionId() {
        this.sessionId = UUID.randomUUID().toString();
    }

    int connectedBackendCount() {
        return (int) backends.values().stream().filter(BackendStateMachine::isConnected).count();
    }

    /**
     * Purely for tests DO NOT USE IN PRODUCTION code!!
     * Sonar will complain if one uses this in prod code listen to it.
     */
    @VisibleForTesting
    void forceState(ProxyChannelState state, KafkaProxyFrontendHandler frontendHandler, @Nullable Map<UpstreamEndpoint, BackendStateMachine> backends) {
        LOGGER.info("Forcing state to {} with {} and {}", state, frontendHandler, backends);
        this.state = state;
        this.frontendHandler = frontendHandler;
        this.backends.clear();
        if (backends != null) {
            this.backends.putAll(backends);
        }
    }

    @Override
    public String toString() {
        return "ProxyChannelStateMachine{" +
                "state=" + state +
                ", clientReadsBlocked=" + clientReadsBlocked +
                ", serverReadsBlocked=" + serverReadsBlocked +
                ", sessionId='" + sessionId + '\'' +
                ", clusters=" + backends.keySet() +
                ", connectedBackends=" + connectedBackendCount() +
                '}';
    }

    public String currentState() {
        return this.state().getClass().getSimpleName();
    }

    public Router router() {
        return binding.endpointGateway().router();
    }

    public void onServerWritable() {
        if (clientReadsBlocked) {
            clientReadsBlocked = false;
            if (clientToProxyBackpressureTimer != null) {
                clientToProxyBackpressureTimer.stop(clientToProxyBackPressureMeter);
                clientToProxyBackpressureTimer = null;
            }
            Objects.requireNonNull(frontendHandler).relieveBackpressure();
        }
    }

    /**
     * Notify the statemachine that the client channel has an active TCP connection.
     * @param frontendHandler with active connection
     */
    void onClientActive(KafkaProxyFrontendHandler frontendHandler) {
        if (STARTING_STATE.equals(this.state)) {
            this.frontendHandler = frontendHandler;
            allocateSessionId(); // this is just keeping the tooling happy it should never be null at this point
            LOGGER.atDebug()
                    .setMessage("Allocated session ID: {} for downstream connection from {}:{}")
                    .addArgument(sessionId)
                    .addArgument(Objects.requireNonNull(this.frontendHandler).remoteHost())
                    .addArgument(this.frontendHandler.remotePort())
                    .log();
            toClientActive(STARTING_STATE.toClientActive(), frontendHandler);
        }
        else {
            illegalState("Client activation while not in the start state");
        }
    }

    /**
     * Notify the statemachine that the connection to the backend has started.
     * @param upstreamEndpoints to connect to
     */
    void onInitiateConnect(Channel inboundChannel, List<UpstreamEndpoint> upstreamEndpoints) {
        if (state instanceof ProxyChannelState.SelectingServer selectingServerState) {
            toConnecting(inboundChannel, selectingServerState.toConnecting(upstreamEndpoints));
        }
        else {
            illegalState(DUPLICATE_INITIATE_CONNECT_ERROR);
        }
    }

    void illegalState(String msg) {
        if (!(state instanceof Closed)) {
            LOGGER.error("Unexpected event while in {} message: {}, closing channels with no client response.", state, msg);
            toClosed(null);
        }
    }

    void clientReadComplete() {
        if (state instanceof ProxyChannelState.Forwarding) {
            flushToBackends();
        }
    }

    /**
     * The proxy has received something from the client. The current state of the session determines what happens to it.
     * @param msg the RPC received from the downstream client
     */
    void onClientRequest(Object msg) {
        Objects.requireNonNull(frontendHandler);
        if (state() instanceof ProxyChannelState.Forwarding) { // post-backend connection
            messageFromClient(msg);
        }
        else {
            handleMessageBeforeForwarding(msg);
        }
    }

    void messageFromClient(Object msg) {
        if (state instanceof ProxyChannelState.Forwarding) {
            forwardToUpstreams(msg);
        }
    }

    /**
     * on first client read, the state machine transitions as follows:
     * 1. msg is {@link HAProxyMessage} => clientActive -> haProxy -> selectingServer
     * 2. msg is ApiVersions request => clientActive -> selectingServer
     *
     * Summary: the first request triggers the server selection and connection process.
     * @param msg the RPC received from the downstream client
     */
    private void handleMessageBeforeForwarding(Object msg) {
        Objects.requireNonNull(frontendHandler).bufferMsg(msg);
        if (state instanceof ProxyChannelState.ClientActive clientActive) {
            onClientRequestInClientActiveState(msg, clientActive);
        }
        else if (state instanceof ProxyChannelState.HaProxy haProxy) {
            transitionClientRequest(msg, haProxy::toSelectingServer);
        }
        else {
            illegalState("Unexpected message received: " + "message class=" + msg.getClass());
        }
    }

    private void onClientRequestInClientActiveState(Object msg, ProxyChannelState.ClientActive clientActive) {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            toHaProxy(clientActive.toHaProxy(haProxyMessage));
        }
        else {
            transitionClientRequest(msg, clientActive::toSelectingServer);
        }
    }

    private void transitionClientRequest(
            Object msg,
            Function<DecodedRequestFrame<ApiVersionsRequestData>, ProxyChannelState.SelectingServer> selectingServerFactory) {
        if (isApiVersionsRequest(msg)) {
            // We know it's an API Versions request even if the compiler doesn't
            @SuppressWarnings("unchecked")
            DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
            toSelectingServer(selectingServerFactory.apply(apiVersionsFrame));
        }
        else if (msg instanceof RequestFrame) {
            toSelectingServer(selectingServerFactory.apply(null));
        }
        else {
            illegalState("Unexpected message type before forwarding: " + msg.getClass());
        }
    }

    @SuppressWarnings("java:S5738")
    void onClientException(@Nullable Throwable cause, boolean tlsEnabled) {
        ApiException errorCodeEx;
        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            String tlsHint;
            tlsHint = tlsEnabled
                    ? ""
                    : " Possible unexpected TLS handshake? When connecting via TLS from your client, make sure to enable TLS for the Kroxylicious gateway ("
                            + StableKroxyliciousLinkGenerator.INSTANCE.errorLink(StableKroxyliciousLinkGenerator.CLIENT_TLS)
                            + ").";
            LOGGER.warn(
                    "Received over-sized frame from the client, max frame size bytes {}, received frame size bytes {} "
                            + "(hint: {} Other possible causes are: an oversized Kafka frame, or something unexpected like an HTTP request.)",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            LOGGER.atWarn()
                    .setCause(LOGGER.isDebugEnabled() ? cause : null)
                    .addArgument(cause != null ? cause.getMessage() : "")
                    .log("Exception from the client channel: {}. Increase log level to DEBUG for stacktrace");
            errorCodeEx = Errors.UNKNOWN_SERVER_ERROR.exception();
        }
        clientToProxyErrorCounter.increment();
        toClosed(errorCodeEx);
    }

    // ==================== Cluster Registration ====================

    /**
     * Register a cluster with explicit node ID offset.
     *
     * @param upstreamEndpoint service endpoint for this cluster
     */
    void addServiceEndpoint(UpstreamEndpoint upstreamEndpoint) {
        LOGGER.info("{}: Adding service endpoint {}", sessionId, upstreamEndpoint);
        if (backends.containsKey(upstreamEndpoint)) {
            throw new IllegalArgumentException("Cluster already registered: " + upstreamEndpoint.getHostPort());
        }

        if (serviceEndpoints.containsKey(upstreamEndpoint.targetCluster().name())) {
            // throw exception that only one service endpoint per cluster is allowed
            throw new IllegalArgumentException("Only one service endpoint per cluster is allowed: " + upstreamEndpoint.getHostPort());
        }

        Counter connectionCounter = Metrics.proxyToServerConnectionCounter(virtualClusterName, null).withTags();
        Counter errorCounter = Metrics.proxyToServerErrorCounter(virtualClusterName, null).withTags();
        Timer backpressureTimer = Metrics.serverToProxyBackpressureTimer(virtualClusterName, null).withTags();

        BackendStateMachine backend = new BackendStateMachine(
                upstreamEndpoint,
                this,
                socketFrameMaxSizeBytes,
                logNetwork,
                logFrames,
                connectionCounter,
                errorCounter,
                backpressureTimer);

        backends.put(upstreamEndpoint, backend);
        serviceEndpoints.put(upstreamEndpoint.targetCluster().name(), upstreamEndpoint);
    }

    // ==================== Accessors ====================

    public String sessionId() {
        return Objects.requireNonNull(sessionId);
    }

    public Set<String> clusterIds() {
        return serviceEndpoints.keySet();
    }

    @SuppressWarnings("java:S5738")
    private void toClientActive(
            ProxyChannelState.ClientActive clientActive,
            KafkaProxyFrontendHandler frontendHandler) {
        setState(clientActive);
        frontendHandler.inClientActive();

        clientToProxyConnectionCounter.increment();
        clientToProxyConnectionToken.acquire();
    }

    @SuppressWarnings("java:S5738")
    private void toConnecting(Channel inboundChannel, ProxyChannelState.Connecting connecting) {
        setState(connecting);
        Objects.requireNonNull(frontendHandler).inConnecting();
        connecting.upstreamEndpoints().forEach(this::addServiceEndpoint);

        // Connect all concurrently
        Objects.requireNonNull(inboundChannel);
        connectAll(inboundChannel)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        LOGGER.warn("{}: Error connecting to upstream clusters: {}", sessionId, t.getMessage());
                        proxyToServerErrorCounter.increment();
                        toClosed(t);
                        return;
                    }
                    proxyToServerConnectionCounter.increment();
                });
    }

    private void toHaProxy(ProxyChannelState.HaProxy haProxy) {
        setState(haProxy);
    }

    private void toSelectingServer(ProxyChannelState.SelectingServer selectingServer) {
        setState(selectingServer);
        Objects.requireNonNull(frontendHandler).inSelectingServer();
    }

    private void toForwarding(ProxyChannelState.Forwarding forwarding) {
        setState(forwarding);
        Objects.requireNonNull(frontendHandler).inForwarding();
        proxyToServerConnectionToken.acquire();
    }

    public Collection<BackendStateMachine> allBackends() {
        return Collections.unmodifiableCollection(backends.values());
    }

    @Nullable
    public BackendStateMachine getBackend(String clusterId) {
        UpstreamEndpoint upstreamEndpoint = serviceEndpoints.get(clusterId);
        return getBackend(upstreamEndpoint);
    }

    @Nullable
    public BackendStateMachine getBackend(UpstreamEndpoint endpoint) {
        return backends.getOrDefault(endpoint, null);
    }

    public boolean isMultiCluster() {
        return backends.size() > 1;
    }

    public boolean isAnyConnected() {
        return anyConnected;
    }

    public boolean areAllConnected() {
        // no null value or not connected value means not all connected

        if (backends.isEmpty()) {
            return false;
        }

        return backends.values().stream()
                .allMatch(b -> Objects.nonNull(b) && b.isConnected());
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
    public void forwardToUpstreams(Object msg) {
        List<UpstreamEndpoint> msgTargets;
        if (msg instanceof Frame frame) {
            msgTargets = binding.upstreamServiceEndpoints(ApiKeys.forId(frame.apiKeyId()));
            if (!aggregationCorrelationManager.containsKey(frame.correlationId())) {
                aggregationCorrelationManager.put(frame.correlationId(), new ResponseAggregationContext<>(msgTargets.size(), this.reconciler));
            }
        }
        else {
            // todo: do we need to handle non-Frame messages here? should we introduce default endpoint in binding?
            throw new IllegalArgumentException("Invalid message type: " + msg.getClass());
        }

        List<BackendStateMachine> msgBackends = msgTargets.stream()
                .map(this::getBackend).toList();
        // if any backend is null, we have an unknown cluster
        if (msgBackends.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("Unknown cluster for message: " + msg);
        }

        // there may be fewer backends required for a message than total backends
        boolean allConnected = msgBackends.stream().allMatch(BackendStateMachine::isConnected);

        if (!allConnected) {
            // todo: should we wait for connections to complete instead?
            throw new IllegalStateException("Not all backends connected for message: " + msg);
        }

        // When sending to multiple backends, we need to retain the message for each
        // additional backend. Netty's MessageToByteEncoder releases the message after
        // encoding, so without retain() the second backend would get a released buffer.
        int backendCount = msgBackends.size();
        if (backendCount > 1) {
            ReferenceCountUtil.retain(msg, backendCount - 1);
        }

        int sentCount = 0;
        try {
            for (BackendStateMachine backend : msgBackends) {
                if (backend == null) { // this should never happen
                    throw new IllegalArgumentException("Unknown cluster for message: " + msg);
                }

                if (!backend.isConnected()) {
                    throw new IllegalStateException("backend not connected for cluster: " + backend.clusterId());
                }

                backend.forwardToServer(msg);
                sentCount++;
            }
        }
        catch (Exception e) {
            // Release any unused retained references on error
            int unusedRefs = backendCount - 1 - sentCount;
            if (unusedRefs > 0) {
                ReferenceCountUtil.release(msg, unusedRefs);
            }
            throw e;
        }
    }

    /**
     * Forward a message to a specific cluster.
     */
    public void forwardToUpstream(String clusterId, Object msg) {
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
    public void flushToBackends() {
        for (BackendStateMachine backend : backends.values()) {
            if (backend.isConnected()) {
                backend.flushToServer();
            }
        }
    }

    // ==================== Backpressure Management ====================

    /**
     * Client is slow, apply backpressure to backends.
     */
    public void onClientUnwritable() {
        applyBackpressureToAll();
    }

    /**
     * Client caught up, relieve backend backpressure.
     */
    public void onClientWritable() {
        if (serverReadsBlocked) {
            serverReadsBlocked = false;
            if (serverBackpressureTimer != null) {
                serverBackpressureTimer.stop(serverToProxyBackpressureMeter);
                serverBackpressureTimer = null;
            }
            relieveBackpressureFromAll();
        }
    }

    void onClientInactive() {
        toClosed(null);
    }

    /**
     * Apply backpressure to all backends (client is slow).
     */
    public void applyBackpressureToAll() {
        // todo: if serverReadsBlocked, no need to apply backpressure yet
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

        LOGGER.atDebug()
                .setMessage("{}: Upstream connection to {} established for client at {}:{}. Total Upstreams connected: {}/{}")
                .addArgument(sessionId)
                .addArgument(backend.endpoint().host())
                .addArgument(Objects.requireNonNull(this.frontendHandler).remoteHost())
                .addArgument(this.frontendHandler.remotePort())
                .addArgument(connectedBackendCount())
                .addArgument(backends.size())
                .log();

        this.anyConnected = true;

        // if all backends are connected, notify session state machine
        if (areAllConnected()) {
            if (state() instanceof ProxyChannelState.Connecting connectedState) {
                toForwarding(connectedState.toForwarding());
            }
            else {
                illegalState("Server became active while not in the connecting state");
            }
        }
    }

    void onBackendFailed(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend failed: {} - {}", sessionId, backend.endpoint().host(), cause.getMessage());
        toClosed(cause);
    }

    void onBackendClosed(BackendStateMachine backend) {
        LOGGER.debug("{}: Backend closed: {}", sessionId, backend.clusterId());

        // Update anyConnected status
        this.anyConnected = backends.values().stream()
                .anyMatch(BackendStateMachine::isConnected);

        // todo: should we close the client connection when a backend closes or only if all backends close?
        toClosed(null);
    }

    void onBackendError(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend error: {} - {}", sessionId, backend.clusterId(), cause.getMessage());
        toClosed(cause);
    }

    void onBackendResponse(BackendStateMachine backend, Object msg) {
        // Forward response to session state machine which routes to frontend
        // aggregate responses based on correlation counter
        // todo: to ensure decoded response, the correlation manager should say decodeResponse=true for aggregated requests
        if (msg instanceof DecodedResponseFrame<?> frame) {
            ResponseAggregationContext aggContext = aggregationCorrelationManager.get(frame.correlationId());
            aggContext.addMessage(backend.targetCluster(), frame);
            int remaining = aggContext.remainingResponses();
            LOGGER.debug("{}: Received response for correlationId {}. Remaining: {}",
                    sessionId, frame.correlationId(), remaining);
            if (remaining > 0) {
                LOGGER.debug("{}: Waiting for more responses for correlationId {}. Remaining: {}",
                        sessionId, frame.correlationId(), remaining);
                return;
            }

            aggregationCorrelationManager.remove(frame.correlationId());

            LOGGER.debug("{}: All responses received for correlationId {}. Preparing aggregated response.",
                    sessionId, frame.correlationId());

            ApiMessageAggregator<?> aggregator = router().aggregator(frame.apiKey());
            if (aggregator != null) {
                aggregator.aggregate(aggContext).whenComplete((aggregatedResponse, throwable) -> {
                    if (throwable != null) {
                        // todo: better error handling
                        LOGGER.error("{}: Error aggregating responses for correlationId {}: {}",
                                sessionId, frame.correlationId(), ((Throwable) throwable).getMessage());
                    }
                    else {
                        var aggregatedMsg = new DecodedResponseFrame<>(
                                frame.apiVersion(),
                                frame.correlationId(),
                                frame.header(),
                                (ApiMessage) aggregatedResponse);
                        LOGGER.debug("{}: Aggregated response for correlationId {} using {} aggregator.",
                                sessionId, frame.correlationId(), aggregator.getClass().getSimpleName());
                        Objects.requireNonNull(frontendHandler).forwardToClient(aggregatedMsg);
                    }
                });

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
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);
    }

    void onBackendReadComplete(BackendStateMachine backend) {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    void onBackendWritable(BackendStateMachine backend) {
        // Only relieve client backpressure when ALL connected backends are writable
        if (!clientReadsBlocked) {
            return;
        }

        boolean allBackendsWritable = allBackends().stream()
                .filter(BackendStateMachine::isConnected)
                .allMatch(BackendStateMachine::isChannelWritable);

        if (allBackendsWritable) {
            clientReadsBlocked = false;
            if (clientBackpressureTimer != null) {
                clientBackpressureTimer.stop(clientToProxyBackPressureMeter);
                clientBackpressureTimer = null;
            }
            Objects.requireNonNull(frontendHandler).relieveBackpressure();
        }
    }

    void onBackendUnwritable(BackendStateMachine backend) {
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            clientBackpressureTimer = Timer.start();
            Objects.requireNonNull(frontendHandler).applyBackpressure();
        }
    }

    // ==================== Cleanup ====================

    /**
     * Close all backend connections.
     */
    private void closeAllBackends() {
        LOGGER.debug("{}: Closing all backends", sessionId);
        for (BackendStateMachine backend : backends.values()) {
            backend.close();
        }
    }

    private void toClosed(@Nullable Throwable errorCodeEx) {
        if (state instanceof Closed) {
            return;
        }

        setState(new Closed());
        // Close the server connection
        closeAllBackends();
        // proxyToServerConnectionToken.release();

        // Close the client connection with any error code
        if (frontendHandler != null) { // Can be null if the error happens before clientActive (unlikely but possible)
            frontendHandler.inClosed(errorCodeEx);
            clientToProxyConnectionToken.release();
        }
    }

    private void setState(ProxyChannelState state) {
        LOGGER.trace("{} transitioning to {}", this, state);
        this.state = state;
    }

    private static boolean isApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame
                && ((DecodedRequestFrame<?>) msg).apiKey() == ApiKeys.API_VERSIONS;
    }
}

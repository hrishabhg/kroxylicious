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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages multiple backend cluster connections for a single client session.
 *
 * <p>This class serves as the bridge between the client session state machine
 * and the per-cluster backend state machines. It provides:</p>
 * <ul>
 *   <li>Cluster registration and lookup</li>
 *   <li>Connection lifecycle management (connect one, connect all)</li>
 *   <li>Aggregate operations (forward to primary, forward to specific cluster)</li>
 *   <li>Backpressure coordination across clusters</li>
 *   <li>Response routing back to the client</li>
 * </ul>
 *
 * <p>For single-cluster deployments, this manager contains exactly one backend.
 * For multi-cluster, it contains one backend per configured cluster.</p>
 */
public class ClusterConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConnectionManager.class);

    private final String sessionId;
    private final String virtualClusterName;
    private final ClientSessionStateMachine sessionStateMachine;
    private final int socketFrameMaxSizeBytes;
    private final boolean logNetwork;
    private final boolean logFrames;

    // Cluster backends
    private final Map<String, BackendStateMachine> backends = new ConcurrentHashMap<>();
    private volatile String primaryClusterId;
    private int nextNodeIdOffset = 0;

    // Connection state
    private volatile boolean anyConnected = false;

    public ClusterConnectionManager(
            String sessionId,
            String virtualClusterName,
            ClientSessionStateMachine sessionStateMachine,
            int socketFrameMaxSizeBytes,
            boolean logNetwork,
            boolean logFrames) {
        this.sessionId = Objects.requireNonNull(sessionId);
        this.virtualClusterName = Objects.requireNonNull(virtualClusterName);
        this.sessionStateMachine = Objects.requireNonNull(sessionStateMachine);
        this.socketFrameMaxSizeBytes = socketFrameMaxSizeBytes;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
    }

    // ==================== Cluster Registration ====================

    /**
     * Register a cluster with auto-assigned node ID offset.
     * First registered cluster becomes the primary.
     *
     * @param clusterId unique identifier for this cluster
     * @param target target cluster configuration
     * @return the created backend state machine
     */
    public BackendStateMachine addCluster(String clusterId, TargetCluster target) {
        return addCluster(clusterId, target, nextNodeIdOffset);
    }

    /**
     * Register a cluster with explicit node ID offset.
     *
     * @param clusterId unique identifier for this cluster
     * @param target target cluster configuration
     * @param nodeIdOffset offset to apply to node IDs from this cluster
     * @return the created backend state machine
     */
    public BackendStateMachine addCluster(String clusterId, TargetCluster target, int nodeIdOffset) {
        if (backends.containsKey(clusterId)) {
            throw new IllegalArgumentException("Cluster already registered: " + clusterId);
        }

        Counter connectionCounter = Metrics.proxyToServerConnectionCounter(virtualClusterName, null).withTags();
        Counter errorCounter = Metrics.proxyToServerErrorCounter(virtualClusterName, null).withTags();
        Timer backpressureTimer = Metrics.serverToProxyBackpressureTimer(virtualClusterName, null).withTags();

        BackendStateMachine backend = new BackendStateMachine(
                clusterId,
                target,
                nodeIdOffset,
                this,
                socketFrameMaxSizeBytes,
                logNetwork,
                logFrames,
                connectionCounter,
                errorCounter,
                backpressureTimer);

        backends.put(clusterId, backend);

        // First cluster is primary
        if (primaryClusterId == null) {
            primaryClusterId = clusterId;
        }

        // Track next offset (assuming 1000 node IDs per cluster)
        nextNodeIdOffset = Math.max(nextNodeIdOffset, nodeIdOffset + 1000);

        LOGGER.debug("{}: Registered cluster {} with nodeIdOffset {}",
                sessionId, clusterId, nodeIdOffset);

        return backend;
    }

    /**
     * Set which cluster is the primary (default for routing).
     */
    public void setPrimaryCluster(String clusterId) {
        if (!backends.containsKey(clusterId)) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterId);
        }
        this.primaryClusterId = clusterId;
    }

    // ==================== Accessors ====================

    public String sessionId() {
        return sessionId;
    }

    public Set<String> clusterIds() {
        return Collections.unmodifiableSet(backends.keySet());
    }

    public Collection<BackendStateMachine> allBackends() {
        return Collections.unmodifiableCollection(backends.values());
    }

    @Nullable
    public BackendStateMachine getBackend(String clusterId) {
        return backends.get(clusterId);
    }

    @Nullable
    public BackendStateMachine primaryBackend() {
        return primaryClusterId != null ? backends.get(primaryClusterId) : null;
    }

    public String primaryClusterId() {
        return primaryClusterId;
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

    // ==================== Connection Management ====================

    /**
     * Connect to a specific cluster.
     *
     * @param clusterId cluster to connect to
     * @param inboundChannel client channel (for event loop)
     * @param sslContext optional TLS context
     * @return future that completes when connected
     */
    public CompletableFuture<BackendStateMachine> connectCluster(
            String clusterId,
            Channel inboundChannel,
            Optional<SslContext> sslContext) {

        BackendStateMachine backend = backends.get(clusterId);
        if (backend == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unknown cluster: " + clusterId));
        }

        return backend.connect(inboundChannel, sslContext);
    }

    /**
     * Connect to the primary cluster.
     */
    public CompletableFuture<BackendStateMachine> connectPrimary(
            Channel inboundChannel,
            Optional<SslContext> sslContext) {

        if (primaryClusterId == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No primary cluster configured"));
        }

        return connectCluster(primaryClusterId, inboundChannel, sslContext);
    }

    /**
     * Connect to all registered clusters concurrently.
     *
     * @param inboundChannel client channel
     * @return future that completes when ALL clusters are connected
     */
    public CompletableFuture<Void> connectAll(Channel inboundChannel) {

        List<CompletableFuture<BackendStateMachine>> futures = new ArrayList<>();

        for (BackendStateMachine backend : backends.values()) {
            Optional<SslContext> ssl = VirtualClusterModel.buildUpstreamSslContext(backend.targetCluster().tls());
            futures.add(backend.connect(inboundChannel, ssl));
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    /**
     * Connect to all clusters, succeeding when ANY one connects.
     * Useful for failover scenarios.
     */
    public CompletableFuture<BackendStateMachine> connectAny(
            Channel inboundChannel,
            Map<String, Optional<SslContext>> sslContexts) {

        CompletableFuture<BackendStateMachine> result = new CompletableFuture<>();
        List<CompletableFuture<BackendStateMachine>> futures = new ArrayList<>();

        for (BackendStateMachine backend : backends.values()) {
            Optional<SslContext> ssl = sslContexts.getOrDefault(
                    backend.clusterId(), Optional.empty());
            CompletableFuture<BackendStateMachine> future = backend.connect(inboundChannel, ssl);
            futures.add(future);

            // Complete result on first success
            future.thenAccept(result::complete);
        }

        // If all fail, complete exceptionally
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .exceptionally(t -> {
                    if (!result.isDone()) {
                        result.completeExceptionally(t);
                    }
                    return null;
                });

        return result;
    }

    // ==================== Request Forwarding ====================

    /**
     * Forward a message to the primary cluster.
     */
    public void forwardToServer(Object msg) {
        BackendStateMachine primary = primaryBackend();
        if (primary == null || !primary.isConnected()) {
            throw new IllegalStateException("Primary cluster not connected");
        }
        primary.forwardToServer(msg);
    }

    /**
     * Forward a message to a specific cluster.
     */
    public void forwardToServer(String clusterId, Object msg) {
        BackendStateMachine backend = backends.get(clusterId);
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
        return "ClusterConnectionManager{" +
                "sessionId='" + sessionId + '\'' +
                ", clusters=" + backends.keySet() +
                ", primaryClusterId='" + primaryClusterId + '\'' +
                ", anyConnected=" + anyConnected +
                '}';
    }
}
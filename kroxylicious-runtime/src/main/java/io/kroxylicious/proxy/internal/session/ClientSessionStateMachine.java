/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.session;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.Channel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.RequestFrame;
import io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler;
import io.kroxylicious.proxy.internal.codec.FrameOversizedException;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.internal.util.StableKroxyliciousLinkGenerator;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.ServiceEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * State machine managing a single client session.
 *
 * <p>This class manages client session lifecycle and delegates backend connection
 * orchestration to {@link ClusterConnectionManager}.</p>
 *
 * <h2>Responsibilities</h2>
 * <ul>
 *   <li><b>Session Lifecycle:</b> Handles state transitions (Startup → Routing → Closed)</li>
 *   <li><b>Protocol Handling:</b> ApiVersions negotiation, PROXY protocol, SASL</li>
 *   <li><b>Backpressure:</b> Coordinates client/backend backpressure via ConnectionManager</li>
 * </ul>
 *
 * <h2>State Flow</h2>
 * <pre>
 *   Startup → ClientActive → [HaProxy] → [ApiVersions] → Routing → Closed
 * </pre>
 *
 * <h2>Architecture</h2>
 * <pre>
 *   KafkaProxyFrontendHandler (Netty I/O)
 *           │
 *           ▼
 *   ClientSessionStateMachine (Session Lifecycle)
 *           │
 *           ▼
 *   ClusterConnectionManager (Backend Orchestration)
 *           │
 *     ┌─────┼─────┐
 *     ▼     ▼     ▼
 *   BackendStateMachine (Per-cluster State)
 * </pre>
 */
public class ClientSessionStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSessionStateMachine.class);

    // Session identity
    private @Nullable String sessionId;
    private final String virtualClusterName;
    private final EndpointBinding endpointBinding;
    private final @Nullable Integer nodeId;

    // State
    private volatile ClientSessionState state = ClientSessionState.Startup.INSTANCE;

    // Handlers
    private @Nullable KafkaProxyFrontendHandler frontendHandler;

    // Connection manager - created at session start, orchestrates backend connections
    private ClusterConnectionManager connectionManager;

    // Virtual cluster configuration (set on first connection initiation)
    private @Nullable VirtualClusterModel virtualClusterModel;

    // Backpressure tracking (aggregate across all backends)
    @VisibleForTesting
    boolean clientReadsBlocked;
    private @Nullable Timer.Sample clientBackpressureTimer;

    // Metrics
    private final Counter clientToProxyConnectionCounter;
    private final Counter clientToProxyErrorCounter;
    private final Timer clientToProxyBackpressureTimer;

    public ClientSessionStateMachine(String virtualClusterName, EndpointBinding endpointBinding) {
        this.virtualClusterName = virtualClusterName;
        this.endpointBinding = endpointBinding;
        this.nodeId = endpointBinding.nodeId();

        this.clientToProxyConnectionCounter = Metrics.clientToProxyConnectionCounter(virtualClusterName, nodeId).withTags();
        this.clientToProxyErrorCounter = Metrics.clientToProxyErrorCounter(virtualClusterName, nodeId).withTags();
        this.clientToProxyBackpressureTimer = Metrics.clientToProxyBackpressureTimer(virtualClusterName, nodeId).withTags();
    }

    // ==================== Accessors ====================

    public String sessionId() {
        return Objects.requireNonNull(sessionId, "Session ID not yet allocated");
    }

    public ClientSessionState state() {
        return state;
    }

    public String currentStateName() {
        return state.getClass().getSimpleName();
    }

    @Nullable
    public ClusterConnectionManager connectionManager() {
        return connectionManager;
    }

    public boolean isRouting() {
        return state instanceof ClientSessionState.Routing;
    }

    public boolean isClosed() {
        return state instanceof ClientSessionState.Closed;
    }

    // ==================== Client Lifecycle Events ====================

    /**
     * Called when client TCP connection becomes active.
     */
    public void onClientActive(KafkaProxyFrontendHandler frontend) {
        if (!(state instanceof ClientSessionState.Startup)) {
            illegalState("Client activation in wrong state");
            return;
        }

        this.frontendHandler = frontend;
        this.sessionId = UUID.randomUUID().toString();

        LOGGER.debug("Session {} started from {}:{}",
                sessionId, frontend.remoteHost(), frontend.remotePort());

        ClientSessionState.ClientActive clientActive = ((ClientSessionState.Startup) state).toClientActive();
        setState(clientActive);

        clientToProxyConnectionCounter.increment();
        frontend.inClientActive();
    }

    /**
     * Called when client connection becomes inactive.
     */
    public void onClientInactive() {
        if (!(state instanceof ClientSessionState.Closed)) {
            LOGGER.debug("{}: Client disconnected", sessionId);
            toClosed(null);
        }
    }

    /**
     * Called when an exception occurs on the client connection.
     */
    public void onClientException(@Nullable Throwable cause, boolean tlsEnabled) {
        ApiException errorCodeEx;

        if (cause instanceof DecoderException de
                && de.getCause() instanceof FrameOversizedException e) {
            String tlsHint = tlsEnabled ? ""
                    : " Possible TLS mismatch? See " +
                            StableKroxyliciousLinkGenerator.INSTANCE.errorLink(
                                    StableKroxyliciousLinkGenerator.CLIENT_TLS);
            LOGGER.warn("Oversized frame from client: max={}, received={}.{}",
                    e.getMaxFrameSizeBytes(), e.getReceivedFrameSizeBytes(), tlsHint);
            errorCodeEx = Errors.INVALID_REQUEST.exception();
        }
        else {
            LOGGER.warn("{}: Client exception: {}", sessionId,
                    cause != null ? cause.getMessage() : "unknown");
            if (LOGGER.isDebugEnabled() && cause != null) {
                LOGGER.debug("{}: Client exception details", sessionId, cause);
            }
            errorCodeEx = Errors.UNKNOWN_SERVER_ERROR.exception();
        }

        clientToProxyErrorCounter.increment();
        toClosed(errorCodeEx);
    }

    /**
     * Called when a message is received from the client.
     */
    public void onClientRequest(Object msg) {
        if (state instanceof ClientSessionState.Routing) {
            // Normal forwarding path
            messageFromClient(msg);
        }
        else if (!onClientRequestBeforeRouting(msg)) {
            illegalState("Unexpected message type: " +
                    (msg == null ? "null" : msg.getClass().getName()));
        }
    }

    /**
     * Called when client read batch is complete.
     */
    public void onClientReadComplete() {
        if (state instanceof ClientSessionState.Routing && connectionManager != null) {
            connectionManager.flushAll();
        }
    }

    // ==================== Backend Connection Initiation ====================

    /**
     * Called by NetFilter to initiate multi-cluster connections.
     *
     * <p>Delegates to {@link ClusterConnectionManager#initiateMultiClusterConnection}.</p>
     */
    public void onNetFilterInitiateMultiClusterConnect(
                                                       List<ServiceEndpoint> serviceEndpoints,
                                                       List<FilterAndInvoker> filters,
                                                       VirtualClusterModel virtualCluster,
                                                       NetFilter netFilter) {

        if (!(state instanceof ClientSessionState.ApiVersions ||
                state instanceof ClientSessionState.Routing)) {
            illegalState("initiateMultiClusterConnect called in wrong state: " + currentStateName());
            return;
        }

        this.virtualClusterModel = virtualCluster;

        // Create connection manager if not already created
        ensureConnectionManager(virtualCluster);

        // Transition to Routing state
        toRouting();

        // Notify frontend
        frontendHandler.inMultiClusterConnecting(serviceEndpoints, filters, connectionManager);
    }

    /**
     * Ensure connection manager is created.
     */
    private void ensureConnectionManager(VirtualClusterModel virtualCluster) {
        if (connectionManager == null) {
            connectionManager = new ClusterConnectionManager(
                    sessionId,
                    virtualClusterName,
                    this,
                    endpointBinding,
                    virtualCluster.socketFrameMaxSizeBytes(),
                    virtualCluster.isLogNetwork(),
                    virtualCluster.isLogFrames());
        }
    }

    // ==================== Callbacks from ClusterConnectionManager ====================

    void onBackendConnected(BackendStateMachine backend) {
        LOGGER.debug("{}: Backend {} connected", sessionId, backend.clusterId());
        Objects.requireNonNull(frontendHandler).onBackendConnected(backend.clusterId(), backend.serviceEndpoint().getHostPort());
    }

    void onBackendFailed(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend {} failed: {}", sessionId, backend.clusterId(), cause.getMessage());

        // If primary failed and no fallback, close session
        /* todo: re-enable when we have mandatory/delayed-connect backends
        if (connectionManager != null &&
                !connectionManager.isAnyConnected()) {
            toClosed(Errors.UNKNOWN_SERVER_ERROR.exception());
        }*/
        toClosed(cause);
    }

    void onBackendClosed(BackendStateMachine backend) {
        LOGGER.debug("{}: Backend {} closed", sessionId, backend.clusterId());

        // If all backends closed, close session
        if (connectionManager != null && !connectionManager.isAnyConnected()) {
            toClosed(null);
        }
    }

    void onBackendError(BackendStateMachine backend, Throwable cause) {
        LOGGER.warn("{}: Backend {} error: {}", sessionId, backend.clusterId(), cause.getMessage());

        toClosed(Errors.UNKNOWN_SERVER_ERROR.exception());
    }

    void onBackendResponse(BackendStateMachine backend, Object msg) {
        // Forward response to client via frontend
        // todo: there can be responses from multiple backends. before forwarding, we may need to aggregate or select among them
        // todo: or can be handled in frontendhandler.
        Objects.requireNonNull(frontendHandler).forwardToClient(msg);
    }

    void onBackendReadComplete(BackendStateMachine backend) {
        Objects.requireNonNull(frontendHandler).flushToClient();
    }

    void onBackendWritable(BackendStateMachine backend) {
        // Only relieve client backpressure when ALL connected backends are writable
        if (connectionManager == null || !clientReadsBlocked) {
            return;
        }

        boolean allBackendsWritable = connectionManager.allBackends().stream()
                .filter(BackendStateMachine::isConnected)
                .allMatch(BackendStateMachine::isChannelWritable);

        if (allBackendsWritable) {
            clientReadsBlocked = false;
            if (clientBackpressureTimer != null) {
                clientBackpressureTimer.stop(clientToProxyBackpressureTimer);
                clientBackpressureTimer = null;
            }
            Objects.requireNonNull(frontendHandler).relieveBackpressure();
        }
    }

    void onBackendUnwritable(BackendStateMachine backend) {
        // Backend is slow - apply client backpressure
        if (!clientReadsBlocked) {
            clientReadsBlocked = true;
            clientBackpressureTimer = Timer.start();
            Objects.requireNonNull(frontendHandler).applyBackpressure();
        }
    }

    // ==================== Client Backpressure (from client to backend) ====================

    /**
     * Client is slow, apply backpressure to backends.
     */
    public void onClientUnwritable() {
        if (connectionManager != null) {
            connectionManager.applyBackpressureToAll();
        }
    }

    /**
     * Client caught up, relieve backend backpressure.
     */
    public void onClientWritable() {
        if (connectionManager != null) {
            connectionManager.relieveBackpressureFromAll();
        }
    }

    // ==================== Internal Message Handling ====================

    private boolean onClientRequestBeforeRouting(Object msg) {
        Objects.requireNonNull(frontendHandler).bufferMsg(msg);

        if (state instanceof ClientSessionState.ClientActive clientActive) {
            return handleClientActiveRequest(msg, clientActive);
        }
        else if (state instanceof ClientSessionState.HaProxy haProxy) {
            return handleHaProxyRequest(msg, haProxy);
        }
        else if (state instanceof ClientSessionState.ApiVersions apiVersions) {
            return handleApiVersionsRequest(msg, apiVersions);
        }
        return false;
    }

    private boolean handleClientActiveRequest(Object msg, ClientSessionState.ClientActive clientActive) {
        if (msg instanceof HAProxyMessage haProxyMessage) {
            setState(clientActive.toHaProxy(haProxyMessage));
            return true;
        }
        return transitionToRoutingOrApiVersions(msg,
                clientActive::toApiVersions,
                clientActive::toRouting);
    }

    private boolean handleHaProxyRequest(Object msg, ClientSessionState.HaProxy haProxy) {
        return transitionToRoutingOrApiVersions(msg,
                haProxy::toApiVersions,
                haProxy::toRouting);
    }

    private boolean handleApiVersionsRequest(Object msg, ClientSessionState.ApiVersions apiVersions) {
        if (msg instanceof RequestFrame) {
            toSelectingServer(apiVersions.toRouting());
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private boolean transitionToRoutingOrApiVersions(
                                                     Object msg,
                                                     Function<DecodedRequestFrame<ApiVersionsRequestData>, ClientSessionState.ApiVersions> toApiVersions,
                                                     Function<DecodedRequestFrame<ApiVersionsRequestData>, ClientSessionState.Routing> toRouting) {

        if (isApiVersionsRequest(msg)) {
            DecodedRequestFrame<ApiVersionsRequestData> frame = (DecodedRequestFrame<ApiVersionsRequestData>) msg;
            toApiVersionsState(toApiVersions.apply(frame), frame);
            return true;
        }
        else if (msg instanceof RequestFrame) {
            toSelectingServer(toRouting.apply(null));
            return true;
        }
        return false;
    }

    private static boolean isApiVersionsRequest(Object msg) {
        return msg instanceof DecodedRequestFrame<?> frame &&
                frame.apiKey() == ApiKeys.API_VERSIONS;
    }

    private void messageFromClient(Object msg) {
        if (connectionManager != null && connectionManager.isAnyConnected()) {
            connectionManager.forwardToServer(msg);
        }
        else {
            Objects.requireNonNull(frontendHandler).bufferMsg(msg);
        }
    }

    // ==================== State Transitions ====================

    private void toApiVersionsState(
                                    ClientSessionState.ApiVersions apiVersions,
                                    DecodedRequestFrame<ApiVersionsRequestData> frame) {
        setState(apiVersions);
        Objects.requireNonNull(frontendHandler).inApiVersions(frame);
    }

    private void toSelectingServer(ClientSessionState.Routing routing) {
        setState(routing);
        Objects.requireNonNull(frontendHandler).inSelectingServer();
    }

    private void toRouting() {
        if (state instanceof ClientSessionState.ApiVersions apiVersions) {
            setState(apiVersions.toRouting());
        }
        else if (state instanceof ClientSessionState.Routing) {
            // Already in routing
        }
        else {
            illegalState("Cannot transition to Routing from " + currentStateName());
        }
    }

    private void toClosed(@Nullable Throwable errorCodeEx) {
        if (state instanceof ClientSessionState.Closed) {
            return;
        }

        LOGGER.debug("{}: Session closing", sessionId);
        setState(ClientSessionState.Closed.INSTANCE);

        // Close all backends
        if (connectionManager != null) {
            connectionManager.closeAll();
        }

        // Close frontend with error if present
        if (frontendHandler != null) {
            frontendHandler.inClosed(errorCodeEx);
        }
    }

    void illegalState(String msg) {
        if (!(state instanceof ClientSessionState.Closed)) {
            LOGGER.error("{}: Illegal state in {}: {}", sessionId, currentStateName(), msg);
            toClosed(null);
        }
    }

    private void setState(ClientSessionState newState) {
        LOGGER.trace("{}: State {} -> {}", sessionId, currentStateName(),
                newState.getClass().getSimpleName());
        this.state = newState;
    }

    // ==================== Assertions ====================

    public void assertIsRouting(String msg) {
        if (!(state instanceof ClientSessionState.Routing)) {
            illegalState(msg);
        }
    }

    public ClientSessionState.Routing enforceRouting(String errorMessage) {
        if (state instanceof ClientSessionState.Routing routing) {
            return routing;
        }
        throw new IllegalStateException("Expected Routing state but was " +
                currentStateName() + ": " + errorMessage);
    }

    @Override
    public String toString() {
        return "ClientSessionStateMachine{" +
                "sessionId='" + sessionId + '\'' +
                ", state=" + currentStateName() +
                ", clusters=" + (connectionManager != null ? connectionManager.clusterIds() : "[]") +
                ", clientReadsBlocked=" + clientReadsBlocked +
                '}';
    }
}

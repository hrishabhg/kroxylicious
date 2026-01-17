/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.session;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.KafkaProxyBackendHandler;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.ServiceEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the state machine for a single backend cluster connection.
 *
 * <p>Each cluster in a multi-cluster setup has its own BackendStateMachine,
 * allowing independent connection lifecycle, backpressure, and failure handling.</p>
 *
 * <p>This class encapsulates:</p>
 * <ul>
 *   <li>Connection state (Created → Connecting → Connected → Closed)</li>
 *   <li>The Netty channel and pipeline for this cluster</li>
 *   <li>Backpressure state for this specific connection</li>
 *   <li>Request forwarding to this cluster</li>
 * </ul>
 *
 * <p>Works with {@link KafkaProxyBackendHandler} for Netty I/O operations.</p>
 */
public class BackendStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendStateMachine.class);

    private final String clusterId;
    private final TargetCluster targetCluster;
    private final ServiceEndpoint serviceEndpoint;
    private final ClusterConnectionManager connectionManager;
    private final int socketFrameMaxSizeBytes;
    private final boolean logNetwork;
    private final boolean logFrames;

    // Mutable state
    private volatile BackendConnectionState state = BackendConnectionState.Created.INSTANCE;
    private @Nullable KafkaProxyBackendHandler backendHandler;
    private @Nullable Channel channel;
    private @Nullable ChannelHandlerContext serverCtx;

    // Backpressure tracking
    private volatile boolean readsBlocked = false;
    private @Nullable Timer.Sample backpressureTimer;

    // Connection promise - completed when connection is ready
    private final CompletableFuture<BackendStateMachine> connectionFuture = new CompletableFuture<>();

    // Async request tracking for RoutingContext.sendRequest()
    // Uses negative correlation IDs to avoid conflicts with client requests
    private final Map<Integer, CompletableFuture<ApiMessage>> pendingAsyncRequests = new ConcurrentHashMap<>();
    private final AtomicInteger asyncCorrelationIdGenerator = new AtomicInteger(-1);

    // Metrics
    private final Counter connectionCounter;
    private final Counter errorCounter;
    private final Timer backpressureTimer_;

    public BackendStateMachine(
                               ServiceEndpoint serviceEndpoint,
                               ClusterConnectionManager connectionManager,
                               int socketFrameMaxSizeBytes,
                               boolean logNetwork,
                               boolean logFrames,
                               Counter connectionCounter,
                               Counter errorCounter,
                               Timer backpressureTimer) {
        this.clusterId = Objects.requireNonNull(serviceEndpoint.targetCluster().name());
        this.targetCluster = Objects.requireNonNull(serviceEndpoint.targetCluster());
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.socketFrameMaxSizeBytes = socketFrameMaxSizeBytes;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.connectionCounter = connectionCounter;
        this.errorCounter = errorCounter;
        this.backpressureTimer_ = backpressureTimer;
        this.serviceEndpoint = Objects.requireNonNull(serviceEndpoint);
    }

    // ==================== Accessors ====================

    public String clusterId() {
        return clusterId;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public ServiceEndpoint serviceEndpoint() {
        return serviceEndpoint;
    }

    public BackendConnectionState state() {
        return state;
    }

    public boolean isConnected() {
        return state.isConnected();
    }

    public CompletableFuture<BackendStateMachine> connectionFuture() {
        return connectionFuture;
    }

    /**
     * Get the session ID from the connection manager.
     */
    public String sessionId() {
        return connectionManager.sessionId();
    }

    /**
     * Get the Netty handler for this backend connection.
     */
    @Nullable
    public KafkaProxyBackendHandler backendHandler() {
        return backendHandler;
    }

    // ==================== Connection Lifecycle ====================

    /**
     * Initiate connection to this cluster.
     *
     * @param inboundChannel the client channel (used for event loop)
     * @return future that completes when connection is established
     */
    public CompletableFuture<BackendStateMachine> connect(Channel inboundChannel) {

        if (!(state instanceof BackendConnectionState.Created)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cannot connect from state: " + state));
        }

        HostPort target = serviceEndpoint.getHostPort();
        LOGGER.debug("{}: Connecting to cluster {} at {}",
                connectionManager.sessionId(), clusterId, target);

        // Transition to Connecting
        Optional<SslContext> sslContext = VirtualClusterModel.buildUpstreamSslContext(targetCluster.tls());
        BackendConnectionState.Connecting connecting = ((BackendConnectionState.Created) state).toConnecting(target, sslContext);
        setState(connecting);

        // Create backend handler - uses the existing KafkaProxyBackendHandler
        this.backendHandler = new KafkaProxyBackendHandler(this, sslContext);

        // Configure and connect
        Bootstrap bootstrap = configureBootstrap(inboundChannel);
        ChannelFuture connectFuture = bootstrap.connect(target.host(), target.port());
        this.channel = connectFuture.channel();

        // Setup pipeline
        configurePipeline(channel.pipeline(), sslContext, target);

        // Handle connection result
        connectFuture.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                LOGGER.trace("{}: TCP connected to {} for cluster {}",
                        connectionManager.sessionId(), target, clusterId);
                // For non-TLS, transition to Connected immediately
                // For TLS, wait for SslHandshakeCompletionEvent in handler
                if (sslContext.isEmpty()) {
                    onConnectionActive();
                }
            }
            else {
                onConnectionFailed(f.cause());
            }
        });

        return connectionFuture;
    }

    /**
     * Called when the connection becomes active (TCP + optional TLS handshake complete).
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionActive() {
        if (state instanceof BackendConnectionState.Connecting connecting) {
            BackendConnectionState.Connected connected = connecting.toConnected();
            setState(connected);
            connectionCounter.increment();

            LOGGER.info("{}: Connected to cluster {} at {}",
                    connectionManager.sessionId(), clusterId, connecting.target());

            connectionFuture.complete(this);
            connectionManager.onBackendConnected(this);
        }
        else {
            LOGGER.warn("{}: Unexpected onConnectionActive in state {}",
                    connectionManager.sessionId(), state);
        }
    }

    /**
     * Called when connection attempt fails.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionFailed(Throwable cause) {
        if (state instanceof BackendConnectionState.Connecting connecting) {
            BackendConnectionState.Failed failed = connecting.toFailed(cause);
            setState(failed);
            errorCounter.increment();

            LOGGER.warn("{}: Failed to connect to cluster {} at {}: {}",
                    connectionManager.sessionId(), clusterId, connecting.target(), cause.getMessage());

            connectionFuture.completeExceptionally(cause);
            connectionManager.onBackendFailed(this, cause);
        }
    }

    /**
     * Called when an established connection becomes inactive.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionInactive() {
        // Cancel pending async requests
        cancelPendingAsyncRequests(
                new IllegalStateException("Connection closed to cluster " + clusterId));

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            LOGGER.debug("{}: Connection to cluster {} closed",
                    connectionManager.sessionId(), clusterId);
            connectionManager.onBackendClosed(this);
        }
    }

    /**
     * Called when an error occurs on the connection.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionError(Throwable cause) {
        LOGGER.warn("{}: Error on cluster {} connection: {}",
                connectionManager.sessionId(), clusterId, cause.getMessage());
        errorCounter.increment();

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            connectionManager.onBackendError(this, cause);
        }
        else if (state instanceof BackendConnectionState.Connecting) {
            onConnectionFailed(cause);
        }
    }

    /**
     * Close this backend connection.
     */
    public void close() {
        if (state.isTerminal()) {
            return;
        }

        LOGGER.debug("{}: Closing connection to cluster {}",
                connectionManager.sessionId(), clusterId);

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
        }
        else if (state instanceof BackendConnectionState.Connecting) {
            setState(BackendConnectionState.Closed.INSTANCE);
        }

        if (backendHandler != null) {
            backendHandler.inClosed();
        }
        else if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    // ==================== Message Handling (from KafkaProxyBackendHandler) ====================

    /**
     * Called when a message is received from the server.
     * Called from KafkaProxyBackendHandler.channelRead().
     */
    public void onMessageFromServer(Object msg) {
        // First check if this is a response to an async request (RoutingContext.sendRequest)
        if (msg instanceof DecodedResponseFrame<?> responseFrame) {
            if (tryCompleteAsyncRequest(responseFrame)) {
                // This response was for an async request - don't forward to frontend
                return;
            }
        }

        // Not an async request response - forward to connection manager for frontend routing
        connectionManager.onBackendResponse(this, msg);
    }

    /**
     * Called when server read batch is complete.
     * Called from KafkaProxyBackendHandler.channelReadComplete().
     */
    public void onServerReadComplete() {
        connectionManager.onBackendReadComplete(this);
    }

    /**
     * Called when server becomes writable (backpressure relieved).
     * Called from KafkaProxyBackendHandler.channelWritabilityChanged().
     */
    public void onServerWritable() {
        connectionManager.onBackendWritable(this);
    }

    /**
     * Called when server becomes unwritable (backpressure applied).
     * Called from KafkaProxyBackendHandler.channelWritabilityChanged().
     */
    public void onServerUnwritable() {
        connectionManager.onBackendUnwritable(this);
    }

    // ==================== Request Forwarding ====================

    /**
     * Forward a message to this cluster's backend.
     */
    public void forwardToServer(Object msg) {
        if (!state.canSendRequests()) {
            throw new IllegalStateException(
                    "Cannot forward to cluster " + clusterId + " in state: " + state);
        }

        if (backendHandler == null) {
            throw new IllegalStateException("Backend handler not available for cluster " + clusterId);
        }

        backendHandler.forwardToServer(msg);
    }

    /**
     * Flush pending writes to this cluster.
     */
    public void flushToServer() {
        if (backendHandler != null) {
            backendHandler.flushToServer();
        }
    }

    // ==================== Backpressure ====================

    /**
     * Apply backpressure - stop reading from this backend.
     */
    public void applyBackpressure() {
        if (!readsBlocked && backendHandler != null) {
            readsBlocked = true;
            backpressureTimer = Timer.start();
            backendHandler.applyBackpressure();
            LOGGER.trace("{}: Backpressure applied to cluster {}",
                    connectionManager.sessionId(), clusterId);
        }
    }

    /**
     * Relieve backpressure - resume reading from this backend.
     */
    public void relieveBackpressure() {
        if (readsBlocked && backendHandler != null) {
            readsBlocked = false;
            if (backpressureTimer != null) {
                backpressureTimer.stop(backpressureTimer_);
                backpressureTimer = null;
            }
            backendHandler.relieveBackpressure();
            LOGGER.trace("{}: Backpressure relieved from cluster {}",
                    connectionManager.sessionId(), clusterId);
        }
    }

    public boolean isReadsBlocked() {
        return readsBlocked;
    }

    /**
     * Check if this backend's channel is writable (can accept more data).
     *
     * @return true if the channel is writable, false otherwise
     */
    public boolean isChannelWritable() {
        return backendHandler != null && backendHandler.isChannelWritable();
    }

    // ==================== Pipeline Configuration ====================

    @VisibleForTesting
    Bootstrap configureBootstrap(Channel inboundChannel) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    private void configurePipeline(
                                   ChannelPipeline pipeline,
                                   Optional<SslContext> sslContext,
                                   HostPort target) {

        CorrelationManager correlationManager = new CorrelationManager();

        if (logFrames) {
            pipeline.addFirst("frameLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger." + clusterId,
                            LogLevel.INFO));
        }

        // todo: check if null needs to be replaced with a listener
        pipeline.addFirst("responseDecoder",
                new KafkaResponseDecoder(correlationManager, socketFrameMaxSizeBytes, null));
        pipeline.addFirst("requestEncoder",
                new KafkaRequestEncoder(correlationManager, null));

        if (logNetwork) {
            pipeline.addFirst("networkLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamNetworkLogger." + clusterId,
                            LogLevel.INFO));
        }

        sslContext.ifPresent(ssl -> {
            SslHandler handler = ssl.newHandler(pipeline.channel().alloc(), target.host(), target.port());
            pipeline.addFirst("ssl", handler);
        });

        LOGGER.debug("{}: Configured pipeline for cluster {}: {}",
                connectionManager.sessionId(), clusterId, pipeline);
    }

    // ==================== Async Request/Response ====================

    /**
     * Send a request to this cluster and receive the response asynchronously.
     *
     * <p>This method is used by {@code RoutingContext.sendRequest()} for explicit
     * cluster targeting.</p>
     *
     * @param <M> response message type
     * @param header request header
     * @param request request message
     * @param hasResponse whether a response is expected
     * @return CompletableFuture completing with the response
     */
    @SuppressWarnings("unchecked")
    public <M extends ApiMessage> CompletableFuture<M> sendRequest(
                                                                   RequestHeaderData header,
                                                                   ApiMessage request,
                                                                   boolean hasResponse) {

        if (!state.canSendRequests()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cannot send request to cluster " + clusterId + " in state: " + state));
        }

        if (serverCtx == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Server context not available for cluster " + clusterId));
        }

        var apiKey = ApiKeys.forId(request.apiKey());

        // Assign a unique negative correlation ID to avoid conflicts with client requests
        int correlationId = asyncCorrelationIdGenerator.getAndDecrement();
        header.setCorrelationId(correlationId);
        header.setRequestApiKey(apiKey.id);

        LOGGER.debug("{}: Cluster {} sending async {} request with correlationId {}",
                connectionManager.sessionId(), clusterId, apiKey, correlationId);

        // Create promise for the response
        CompletableFuture<ApiMessage> responsePromise = new CompletableFuture<>();

        if (hasResponse) {
            pendingAsyncRequests.put(correlationId, responsePromise);
        }

        // Create a properly framed request
        DecodedRequestFrame<ApiMessage> frame = new DecodedRequestFrame<>(
                header.requestApiVersion(),
                correlationId,
                hasResponse,
                header,
                request);

        // Write the framed request
        Channel outboundChannel = serverCtx.channel();
        if (!outboundChannel.isWritable()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Channel not writable for cluster " + clusterId));
        }

        outboundChannel.writeAndFlush(frame).addListener(future -> {
            if (!future.isSuccess()) {
                pendingAsyncRequests.remove(correlationId);
                responsePromise.completeExceptionally(future.cause());
            }
            else if (!hasResponse) {
                responsePromise.complete(null);
            }
        });

        return (CompletableFuture<M>) responsePromise;
    }

    /**
     * Try to complete a pending async request with a response.
     */
    @SuppressWarnings("unchecked")
    boolean tryCompleteAsyncRequest(DecodedResponseFrame<?> response) {
        int correlationId = response.correlationId();

        // Only negative correlation IDs are for our async requests
        if (correlationId >= 0) {
            return false;
        }

        CompletableFuture<ApiMessage> promise = pendingAsyncRequests.remove(correlationId);
        if (promise != null) {
            LOGGER.debug("{}: Cluster {} completing async request correlationId {}",
                    connectionManager.sessionId(), clusterId, correlationId);
            promise.complete(response.body());
            return true;
        }

        LOGGER.warn("{}: Cluster {} received response for unknown async correlationId {}",
                connectionManager.sessionId(), clusterId, correlationId);
        return false;
    }

    /**
     * Cancel all pending async requests.
     */
    private void cancelPendingAsyncRequests(Throwable cause) {
        if (!pendingAsyncRequests.isEmpty()) {
            LOGGER.debug("{}: Cluster {} cancelling {} pending async requests",
                    connectionManager.sessionId(), clusterId, pendingAsyncRequests.size());

            for (CompletableFuture<ApiMessage> promise : pendingAsyncRequests.values()) {
                promise.completeExceptionally(cause);
            }
            pendingAsyncRequests.clear();
        }
    }

    // ==================== Internal State Management ====================

    private void setState(BackendConnectionState newState) {
        LOGGER.trace("{}: Cluster {} state {} -> {}",
                connectionManager.sessionId(), clusterId, state, newState);
        this.state = newState;
    }

    /**
     * Set the server context. Called from KafkaProxyBackendHandler.channelRegistered().
     */
    public void setServerContext(ChannelHandlerContext ctx) {
        this.serverCtx = ctx;
    }

    /**
     * Report an illegal state condition.
     */
    public void illegalState(String msg) {
        LOGGER.error("{}: Cluster {} illegal state: {}", connectionManager.sessionId(), clusterId, msg);
        close();
    }

    @Override
    public String toString() {
        return "BackendStateMachine{" +
                "clusterId='" + clusterId + '\'' +
                ", state=" + state +
                ", readsBlocked=" + readsBlocked +
                '}';
    }
}

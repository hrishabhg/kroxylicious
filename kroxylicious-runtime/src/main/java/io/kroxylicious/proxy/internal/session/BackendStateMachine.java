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
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.internal.KafkaProxyBackendHandler;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.service.HostPort;
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
 */
public class BackendStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendStateMachine.class);

    private final String clusterId;
    private final TargetCluster targetCluster;
    private final int nodeIdOffset;
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
            String clusterId,
            TargetCluster targetCluster,
            int nodeIdOffset,
            ClusterConnectionManager connectionManager,
            int socketFrameMaxSizeBytes,
            boolean logNetwork,
            boolean logFrames,
            Counter connectionCounter,
            Counter errorCounter,
            Timer backpressureTimer) {
        this.clusterId = Objects.requireNonNull(clusterId);
        this.targetCluster = Objects.requireNonNull(targetCluster);
        this.nodeIdOffset = nodeIdOffset;
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.socketFrameMaxSizeBytes = socketFrameMaxSizeBytes;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.connectionCounter = connectionCounter;
        this.errorCounter = errorCounter;
        this.backpressureTimer_ = backpressureTimer;
    }

    // ==================== Accessors ====================

    public String clusterId() {
        return clusterId;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public int nodeIdOffset() {
        return nodeIdOffset;
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

    @Nullable
    public KafkaProxyBackendHandler backendHandler() {
        return backendHandler;
    }

    // ==================== Connection Lifecycle ====================

    /**
     * Initiate connection to this cluster.
     *
     * @param inboundChannel the client channel (used for event loop)
     * @param sslContext optional SSL context for upstream TLS
     * @return future that completes when connection is established
     */
    public CompletableFuture<BackendStateMachine> connect(
            Channel inboundChannel,
            Optional<SslContext> sslContext) {

        if (!(state instanceof BackendConnectionState.Created)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cannot connect from state: " + state));
        }

        HostPort target = targetCluster.bootstrapServer();
        LOGGER.debug("{}: Connecting to cluster {} at {}",
                connectionManager.sessionId(), clusterId, target);

        // Transition to Connecting
        BackendConnectionState.Connecting connecting =
                ((BackendConnectionState.Created) state).toConnecting(target, sslContext);
        setState(connecting);

        // Create backend handler
        this.backendHandler = new BackendHandlerAdapter(this, sslContext);

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
                // For TLS, wait for SslHandshakeCompletionEvent
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
     */
    void onConnectionActive() {
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
     */
    void onConnectionFailed(Throwable cause) {
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
     */
    void onConnectionInactive() {
        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            LOGGER.debug("{}: Connection to cluster {} closed",
                    connectionManager.sessionId(), clusterId);
            connectionManager.onBackendClosed(this);
        }
    }

    /**
     * Called when an error occurs on the connection.
     */
    void onConnectionError(Throwable cause) {
        LOGGER.warn("{}: Error on cluster {} connection: {}",
                connectionManager.sessionId(), clusterId, cause.getMessage());
        errorCounter.increment();

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            connectionManager.onBackendError(this, cause);
        }
        else if (state instanceof BackendConnectionState.Connecting connecting) {
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

        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
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

        if (serverCtx == null) {
            throw new IllegalStateException("Server context not available for cluster " + clusterId);
        }

        Channel outboundChannel = serverCtx.channel();
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, serverCtx.voidPromise());
        }
        else {
            outboundChannel.writeAndFlush(msg, serverCtx.voidPromise());
        }
    }

    /**
     * Flush pending writes to this cluster.
     */
    public void flushToServer() {
        if (serverCtx != null && serverCtx.channel().isActive()) {
            serverCtx.channel().flush();

            if (!serverCtx.channel().isWritable()) {
                connectionManager.onBackendUnwritable(this);
            }
        }
    }

    // ==================== Backpressure ====================

    /**
     * Apply backpressure - stop reading from this backend.
     */
    public void applyBackpressure() {
        if (!readsBlocked && serverCtx != null) {
            readsBlocked = true;
            backpressureTimer = Timer.start();
            serverCtx.channel().config().setAutoRead(false);
            LOGGER.trace("{}: Backpressure applied to cluster {}",
                    connectionManager.sessionId(), clusterId);
        }
    }

    /**
     * Relieve backpressure - resume reading from this backend.
     */
    public void relieveBackpressure() {
        if (readsBlocked && serverCtx != null) {
            readsBlocked = false;
            if (backpressureTimer != null) {
                backpressureTimer.stop(backpressureTimer_);
                backpressureTimer = null;
            }
            serverCtx.channel().config().setAutoRead(true);
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
     * <p>Used for backpressure: when ANY backend becomes unwritable, we apply
     * backpressure to the client. When ALL backends become writable again,
     * we relieve client backpressure.</p>
     *
     * @return true if the channel is writable, false otherwise
     */
    public boolean isChannelWritable() {
        return serverCtx != null && serverCtx.channel().isWritable();
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
     * cluster targeting. It creates a properly framed request, tracks correlation,
     * and routes the response back to the returned promise.</p>
     *
     * @param <M> response message type
     * @param header request header (requestApiVersion should be set)
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
            // Track this request for response matching
            pendingAsyncRequests.put(correlationId, responsePromise);
        }

        // Create a properly framed request
        DecodedRequestFrame<ApiMessage> frame = new DecodedRequestFrame<>(
                header.requestApiVersion(),
                correlationId,
                hasResponse,
                header,
                request);

        // Write the framed request to the backend channel
        Channel outboundChannel = serverCtx.channel();
        if (!outboundChannel.isWritable()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Channel not writable for cluster " + clusterId));
        }
        outboundChannel.writeAndFlush(frame).addListener(future -> {
            if (!future.isSuccess()) {
                // Write failed - remove from pending and complete exceptionally
                pendingAsyncRequests.remove(correlationId);
                responsePromise.completeExceptionally(future.cause());
            }
            else if (!hasResponse) {
                // No response expected (e.g., acks=0 Produce) - complete immediately
                responsePromise.complete(null);
            }
            // Otherwise, promise will be completed when response arrives
        });

        return (CompletableFuture<M>) responsePromise;
    }

    /**
     * Try to complete a pending async request with a response.
     *
     * @param response the response frame
     * @return true if this response was for an async request and was handled, false otherwise
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
            LOGGER.debug("{}: Cluster {} completing async request with correlationId {}",
                    connectionManager.sessionId(), clusterId, correlationId);
            promise.complete(response.body());
            return true;
        }

        // Correlation not found - might be stale or error
        LOGGER.warn("{}: Cluster {} received response for unknown async correlationId {}",
                connectionManager.sessionId(), clusterId, correlationId);
        return false;
    }

    /**
     * Cancel all pending async requests (e.g., on connection close).
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
        LOGGER.trace("{}: Cluster {} state transition {} -> {}",
                connectionManager.sessionId(), clusterId, state, newState);
        this.state = newState;
    }

    void setServerContext(ChannelHandlerContext ctx) {
        this.serverCtx = ctx;
    }

    @Override
    public String toString() {
        return "BackendStateMachine{" +
                "clusterId='" + clusterId + '\'' +
                ", state=" + state +
                ", nodeIdOffset=" + nodeIdOffset +
                ", readsBlocked=" + readsBlocked +
                '}';
    }

    // ==================== Adapter for KafkaProxyBackendHandler ====================

    /**
     * Adapter that bridges the existing KafkaProxyBackendHandler to BackendStateMachine.
     * This allows us to reuse the existing handler logic while managing state here.
     */
    private static class BackendHandlerAdapter extends KafkaProxyBackendHandler {

        private final BackendStateMachine stateMachine;

        BackendHandlerAdapter(BackendStateMachine stateMachine, Optional<SslContext> sslContext) {
            super(null, sslContext); // We override the state machine interactions
            this.stateMachine = stateMachine;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            stateMachine.setServerContext(ctx);
            super.channelRegistered(ctx);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            LOGGER.trace("Channel active for cluster {}", stateMachine.clusterId());
            if (stateMachine.state instanceof BackendConnectionState.Connecting connecting
                    && connecting.sslContext().isEmpty()) {
                stateMachine.onConnectionActive();
            }
            // For TLS, we wait for SslHandshakeCompletionEvent
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
            if (event instanceof SslHandshakeCompletionEvent sslEvt) {
                if (sslEvt.isSuccess()) {
                    stateMachine.onConnectionActive();
                }
                else {
                    stateMachine.onConnectionFailed(sslEvt.cause());
                }
            }
            super.userEventTriggered(ctx, event);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // Cancel pending async requests before notifying state machine
            stateMachine.cancelPendingAsyncRequests(
                    new IllegalStateException("Connection closed to cluster " + stateMachine.clusterId()));
            stateMachine.onConnectionInactive();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            stateMachine.onConnectionError(cause);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            // First check if this is a response to an async request (RoutingContext.sendRequest)
            if (msg instanceof DecodedResponseFrame<?> responseFrame) {
                if (stateMachine.tryCompleteAsyncRequest(responseFrame)) {
                    // This response was for an async request - don't forward to frontend
                    return;
                }
            }

            // Not an async request response - forward to connection manager for frontend routing
            stateMachine.connectionManager.onBackendResponse(stateMachine, msg);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            super.channelReadComplete(ctx);
            stateMachine.connectionManager.onBackendReadComplete(stateMachine);
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            super.channelWritabilityChanged(ctx);
            if (ctx.channel().isWritable()) {
                stateMachine.connectionManager.onBackendWritable(stateMachine);
            }
            else {
                stateMachine.connectionManager.onBackendUnwritable(stateMachine);
            }
        }
    }
}
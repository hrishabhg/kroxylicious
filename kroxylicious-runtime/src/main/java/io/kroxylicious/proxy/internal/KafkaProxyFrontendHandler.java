/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.filter.NetFilter;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.session.ClientSessionState;
import io.kroxylicious.proxy.internal.session.ClientSessionStateMachine;
import io.kroxylicious.proxy.internal.session.ClusterConnectionManager;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.ServiceEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Handles the downstream/client side of the proxy connection.
 *
 * <p>This handler manages:</p>
 * <ul>
 *   <li>Client connection lifecycle (active, inactive, exceptions)</li>
 *   <li>Message reading from client and forwarding to backends</li>
 *   <li>Response forwarding from backends to client</li>
 *   <li>Backpressure propagation between client and backends</li>
 *   <li>NetFilter callbacks for server selection</li>
 * </ul>
 *
 * <p>Works with {@link ClientSessionStateMachine} for session state management
 * and {@link ClusterConnectionManager} for backend connection management.</p>
 */
public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter
        implements NetFilter.NetFilterContext {

    private static final String NET_FILTER_INVOKED_IN_WRONG_STATE = "NetFilterContext invoked in wrong session state";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    /** Cache ApiVersions response which we use when returning ApiVersions ourselves */
    private static final ApiVersionsResponseData API_VERSIONS_RESPONSE;

    // Configuration
    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualClusterModel virtualClusterModel;
    private final EndpointBinding endpointBinding;
    private final NetFilter netFilter;
    private final DelegatingDecodePredicate dp;
    private final TransportSubjectBuilder subjectBuilder;

    // Session management (replaces old ProxyChannelStateMachine)
    private final ClientSessionStateMachine sessionStateMachine;

    // Connection management (manages one or more backend connections)
    private @Nullable ClusterConnectionManager connectionManager;

    // Client channel context
    private @Nullable ChannelHandlerContext clientCtx;

    // Message buffering before backend connection
    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;

    // Client write state
    private boolean pendingClientFlushes;

    // SNI hostname from TLS handshake
    private @Nullable String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    private boolean pendingReadComplete = true;

    // Client authentication state
    private @Nullable ClientSubjectManager clientSubjectManager;

    // Progression latch for client unblocking
    private int progressionLatch = -1;

    // Filters applied to this session
    private @Nullable List<FilterAndInvoker> filters;

    static {
        var objectMapper = new ObjectMapper();
        try (var parser = KafkaProxyFrontendHandler.class.getResourceAsStream("/ApiVersions-3.2.json")) {
            API_VERSIONS_RESPONSE = ApiVersionsResponseDataJsonConverter.read(objectMapper.readTree(parser), (short) 3);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Constructor.
     *
     * @param netFilter the network filter for server selection
     * @param dp decode predicate for message decoding decisions
     * @param subjectBuilder builds authentication subject from transport
     * @param endpointBinding the endpoint binding for this connection
     * @param sessionStateMachine the session state machine (replaces ProxyChannelStateMachine)
     */
    KafkaProxyFrontendHandler(
                              NetFilter netFilter,
                              DelegatingDecodePredicate dp,
                              TransportSubjectBuilder subjectBuilder,
                              EndpointBinding endpointBinding,
                              ClientSessionStateMachine sessionStateMachine) {
        this.netFilter = netFilter;
        this.dp = dp;
        this.subjectBuilder = Objects.requireNonNull(subjectBuilder);
        this.endpointBinding = endpointBinding;
        this.virtualClusterModel = endpointBinding.endpointGateway().virtualCluster();
        this.sessionStateMachine = sessionStateMachine;
        this.logNetwork = virtualClusterModel.isLogNetwork();
        this.logFrames = virtualClusterModel.isLogFrames();
    }

    // ==================== Accessors ====================

    /**
     * @return the SSL session, or null if a session does not (currently) exist.
     */
    @Nullable
    SSLSession sslSession() {
        SslHandler sslHandler = (SslHandler) this.clientCtx().pipeline().get(SslHandler.class.getName());
        return Optional.ofNullable(sslHandler)
                .map(SslHandler::engine)
                .map(SSLEngine::getSession)
                .orElse(null);
    }

    /**
     * Get the ID of the frontend channel if available.
     * @return {@code null} if the channel is not yet available
     */
    @CheckReturnValue
    @Nullable
    public ChannelId channelId() {
        Channel channel = this.clientCtx != null ? this.clientCtx.channel() : null;
        return channel != null ? channel.id() : null;
    }

    public String remoteHost() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        else {
            return String.valueOf(socketAddress);
        }
    }

    public int remotePort() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getPort();
        }
        else {
            return -1;
        }
    }

    @Nullable
    public ClusterConnectionManager connectionManager() {
        return connectionManager;
    }

    /**
     * Get the client channel.
     * Used by ClientSessionStateMachine when initiating backend connections.
     *
     * @return the client channel
     */
    public Channel clientChannel() {
        return clientCtx().channel();
    }

    @Override
    public String toString() {
        return "KafkaProxyFrontendHandler{" +
                "clientCtx=" + clientCtx +
                ", sessionState=" + sessionStateMachine.currentStateName() +
                ", bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size()) +
                ", pendingClientFlushes=" + pendingClientFlushes +
                ", sniHostname='" + sniHostname + '\'' +
                ", pendingReadComplete=" + pendingReadComplete +
                ", progressionLatch=" + progressionLatch +
                '}';
    }

    // ==================== Netty Channel Callbacks ====================

    /**
     * Netty callback for custom events (SNI, SSL handshake completion).
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof SniCompletionEvent sniCompletionEvent) {
            if (sniCompletionEvent.isSuccess()) {
                this.sniHostname = sniCompletionEvent.hostname();
            }
            else {
                throw new IllegalStateException("SNI failed", sniCompletionEvent.cause());
            }
        }
        else if (event instanceof SslHandshakeCompletionEvent handshakeCompletionEvent
                && handshakeCompletionEvent.isSuccess()) {
            this.clientSubjectManager.subjectFromTransport(sslSession(), subjectBuilder, this::onTransportSubjectBuilt);
        }
        super.userEventTriggered(ctx, event);
    }

    /**
     * Netty callback when client TCP connection is active.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientCtx = ctx;
        this.sessionStateMachine.onClientActive(this);
    }

    /**
     * Netty callback when client TCP connection is closed.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        sessionStateMachine.onClientInactive();
    }

    /**
     * Netty callback for client channel writability changes (backpressure).
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            sessionStateMachine.onClientWritable();
        }
        else {
            sessionStateMachine.onClientUnwritable();
        }
    }

    /**
     * Netty callback when a message is read from client.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        sessionStateMachine.onClientRequest(msg);
    }

    /**
     * Netty callback when client read batch is complete.
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext clientCtx) {
        sessionStateMachine.onClientReadComplete();
    }

    /**
     * Netty callback for client-side exceptions.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        sessionStateMachine.onClientException(cause,
                endpointBinding.endpointGateway().getDownstreamSslContext().isPresent());
    }

    // ==================== Callbacks from ClientSessionStateMachine ====================

    /**
     * Called on entry to ClientActive state.
     */
    public void inClientActive() {
        Channel clientChannel = clientCtx().channel();
        LOGGER.trace("{}: channelActive", clientChannel.id());

        // Initially disable auto-read until we're ready
        clientChannel.config().setAutoRead(false);
        clientChannel.read();

        // Initialize client subject manager
        this.clientSubjectManager = new ClientSubjectManager();
        this.progressionLatch = 2; // Require two events before unblocking

        if (!this.endpointBinding.endpointGateway().isUseTls()) {
            this.clientSubjectManager.subjectFromTransport(null, this.subjectBuilder, this::onTransportSubjectBuilt);
        }

        // Add filters to client pipeline
        addFiltersToPipeline(netFilter.getFilterAndInvokerCollection(),
                clientCtx().pipeline(), clientCtx().channel());
    }

    /**
     * Called on entry to ApiVersions state.
     */
    public void inApiVersions(DecodedRequestFrame<ApiVersionsRequestData> apiVersionsFrame) {
        writeApiVersionsResponse(clientCtx(), apiVersionsFrame);
        this.clientCtx().channel().read();
    }

    /**
     * Called on entry to SelectingServer/Routing state.
     */
    public void inSelectingServer() {
        // NetFilter.selectServer() will callback on initiateConnect()
        // this.netFilter.selectServer(this, routingContext);
        this.sessionStateMachine.assertIsRouting(
                "NetFilter.selectServer() did not callback on NetFilterContext.initiateConnect(): filter='" + this.netFilter + "'");
    }

    /**
     * Called when connecting to multiple clusters.
     * 
     * <p>Note: Connection initiation is handled by {@link ClientSessionStateMachine} which
     * calls {@link ClusterConnectionManager#initiateMultiClusterConnection}. This method
     * only stores references and sets up the decode predicate.</p>
     *
     * @param serviceEndpoints service endpoints
     * @param filters protocol filters
     * @param connectionManager the connection manager (connections already being established)
     */
    public void inMultiClusterConnecting(
                                         List<ServiceEndpoint> serviceEndpoints,
                                         List<FilterAndInvoker> filters,
                                         ClusterConnectionManager connectionManager) {

        this.connectionManager = connectionManager;
        this.filters = filters;

        LOGGER.info("{}: Connecting to {} clusters using filters {}",
                sessionStateMachine.sessionId(), serviceEndpoints.size(), filters);

        // Update decode predicate with filter requirements
        // Connection is initiated by ClientSessionStateMachine.onNetFilterInitiateMultiClusterConnect()
        dp.setDelegate(DecodePredicate.forFilters(filters));
    }

    /**
     * Called when a backend connection is established.
     * Forwards buffered messages and enables client auto-read.
     */
    public void onBackendConnected(String clusterId, HostPort hostPort) {

        LOGGER.debug("{}: Backend#{}.{} connected", sessionStateMachine.sessionId(), clusterId, hostPort);

        // check if all backends are connected
        if (connectionManager != null && !connectionManager.areAllConnected()) {
            LOGGER.debug("{}: waiting for other backends to connect", sessionStateMachine.sessionId());
            return;
        }

        LOGGER.debug("{}: backend connected, forwarding {} buffered messages",
                sessionStateMachine.sessionId(), bufferedMsgs == null ? 0 : bufferedMsgs.size());

        // Forward buffered messages
        if (bufferedMsgs != null && connectionManager != null) {
            for (Object bufferedMsg : bufferedMsgs) {
                connectionManager.forwardToServer(bufferedMsg);
            }
            bufferedMsgs = null;
        }

        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(Objects.requireNonNull(this.clientCtx));
        }

        // Enable auto-read now that we can forward
        onReadyToForwardMore();
    }

    /**
     * Called on entry to Closed state.
     */
    public void inClosed(@Nullable Throwable errorCodeEx) {
        Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isActive()) {
            Object msg = null;
            if (errorCodeEx != null) {
                msg = errorResponse(errorCodeEx);
            }
            if (msg == null) {
                msg = Unpooled.EMPTY_BUFFER;
            }
            inboundChannel.writeAndFlush(msg)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    // ==================== Message Forwarding ====================

    /**
     * Forward a response to the client.
     */
    public void forwardToClient(Object msg) {
        final Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, clientCtx().voidPromise()); // it will start filter chain in reverse direction
            pendingClientFlushes = true;
        }
        else {
            inboundChannel.writeAndFlush(msg, clientCtx().voidPromise());
            pendingClientFlushes = false;
        }
    }

    /**
     * Flush pending writes to client.
     */
    public void flushToClient() {
        final Channel inboundChannel = clientCtx().channel();
        if (pendingClientFlushes) {
            pendingClientFlushes = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            sessionStateMachine.onClientUnwritable();
        }
    }

    /**
     * Buffer a message for later forwarding (before backend is connected).
     */
    public void bufferMsg(Object msg) {
        if (bufferedMsgs == null) {
            bufferedMsgs = new ArrayList<>();
        }
        bufferedMsgs.add(msg);
    }

    // ==================== Backpressure ====================

    /**
     * Apply backpressure to client (stop reading).
     */
    public void applyBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Relieve backpressure from client (resume reading).
     */
    public void relieveBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(true);
        }
    }

    // ==================== NetFilter.NetFilterContext Implementation ====================

    @Override
    public String clientHost() {
        final ClientSessionState.Routing routing = sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        if (routing.haProxyMessage() != null) {
            return routing.haProxyMessage().sourceAddress();
        }
        else {
            return remoteHost();
        }
    }

    @Override
    public int clientPort() {
        final ClientSessionState.Routing routing = sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        if (routing.haProxyMessage() != null) {
            return routing.haProxyMessage().sourcePort();
        }
        else {
            return remotePort();
        }
    }

    @Override
    public SocketAddress srcAddress() {
        sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return clientCtx().channel().remoteAddress();
    }

    @Override
    public SocketAddress localAddress() {
        sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return clientCtx().channel().localAddress();
    }

    @Override
    @Nullable
    public String authorizedId() {
        sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return null;
    }

    @Override
    @Nullable
    public String clientSoftwareName() {
        return sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE).clientSoftwareName();
    }

    @Override
    @Nullable
    public String clientSoftwareVersion() {
        return sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE).clientSoftwareVersion();
    }

    @Override
    @Nullable
    public String sniHostname() {
        sessionStateMachine.enforceRouting(NET_FILTER_INVOKED_IN_WRONG_STATE);
        return sniHostname;
    }

    /**
     * Initiates connection to a single server (backward compatible).
     * Called by NetFilter.
     */
    @Override
    public void initiateConnect(
                                List<ServiceEndpoint> serviceEndpoints,
                                List<FilterAndInvoker> filters) {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend endpoints {} using filters {}",
                    sessionStateMachine.sessionId(), serviceEndpoints, filters);
        }

        sessionStateMachine.onNetFilterInitiateMultiClusterConnect(serviceEndpoints, filters, virtualClusterModel, netFilter);
    }

    public boolean downstreamTlsEnabled() {
        return endpointBinding.endpointGateway().getDownstreamSslContext().isPresent();
    }

    // ==================== Private Helpers ====================

    private void onTransportSubjectBuilt() {
        maybeUnblock();
    }

    private void onReadyToForwardMore() {
        maybeUnblock();
    }

    private void maybeUnblock() {
        if (--this.progressionLatch == 0) {
            unblockClient();
        }
    }

    private void unblockClient() {
        var inboundChannel = clientCtx().channel();
        inboundChannel.config().setAutoRead(true);
        sessionStateMachine.onClientWritable();
    }

    private void writeApiVersionsResponse(ChannelHandlerContext ctx,
                                          DecodedRequestFrame<ApiVersionsRequestData> frame) {
        short apiVersion = frame.apiVersion();
        int correlationId = frame.correlationId();
        ResponseHeaderData header = new ResponseHeaderData()
                .setCorrelationId(correlationId);
        LOGGER.debug("{}: Writing ApiVersions response", ctx.channel());
        ctx.writeAndFlush(new DecodedResponseFrame<>(
                apiVersion, correlationId, header, API_VERSIONS_RESPONSE));
    }

    private void addFiltersToPipeline(
                                      List<FilterAndInvoker> filters,
                                      ChannelPipeline pipeline,
                                      Channel inboundChannel) {

        int num = 0;
        for (var protocolFilter : filters) {
            String handlerName = "filter-" + (++num) + "-" + protocolFilter.filterName();
            pipeline.addBefore(clientCtx().name(),
                    handlerName,
                    new FilterHandler(
                            protocolFilter,
                            20000,
                            sniHostname,
                            virtualClusterModel,
                            inboundChannel,
                            sessionStateMachine,
                            clientSubjectManager,
                            endpointBinding));
        }
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx,
                "clientCtx was null while in state " + this.sessionStateMachine.currentStateName());
    }

    private @Nullable ResponseFrame errorResponse(@Nullable Throwable errorCodeEx) {
        ResponseFrame errorResponse;
        final Object triggerMsg = bufferedMsgs != null && !bufferedMsgs.isEmpty() ? bufferedMsgs.get(0) : null;
        if (errorCodeEx != null && triggerMsg instanceof final DecodedRequestFrame<?> triggerFrame) {
            errorResponse = buildErrorResponseFrame(triggerFrame, errorCodeEx);
        }
        else {
            errorResponse = null;
        }
        return errorResponse;
    }

    private static ResponseFrame buildErrorResponseFrame(
                                                         DecodedRequestFrame<?> triggerFrame,
                                                         Throwable error) {
        var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(),
                responseHeaderData, responseData);
    }
}

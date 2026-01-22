/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SniCompletionEvent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterAndInvoker;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.ResponseFrame;
import io.kroxylicious.proxy.internal.ProxyChannelState.ClientActive;
import io.kroxylicious.proxy.internal.ProxyChannelState.Closed;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.session.ClientSessionStateMachine;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.internal.session.ClientSessionState;
import io.kroxylicious.proxy.internal.session.ClientSessionStateMachine;
import io.kroxylicious.proxy.internal.session.ClusterConnectionManager;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.ServiceEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.ProxyChannelState.Connecting;
import static io.kroxylicious.proxy.internal.ProxyChannelState.Forwarding;
import static io.kroxylicious.proxy.internal.ProxyChannelState.SelectingServer;

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
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    private final boolean logNetwork;
    private final boolean logFrames;
    private final VirtualClusterModel virtualClusterModel;
    private final EndpointBinding endpointBinding;
    private final EndpointReconciler endpointReconciler;
    private final DelegatingDecodePredicate dp;
    private final TransportSubjectBuilder subjectBuilder;
    private final PluginFactoryRegistry pfr;
    private final FilterChainFactory filterChainFactory;
    private final List<NamedFilterDefinition> namedFilterDefinitions;
    private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
    private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

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

    private @Nullable List<FilterAndInvoker> filters;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    // Client authentication state
    private @Nullable ClientSubjectManager clientSubjectManager;

    // Progression latch for client unblocking
    private int progressionLatch = -1;

    /**
     * @return the SSL session, or null if a session does not (currently) exist.
     */
    @Nullable
    SSLSession sslSession() {
        // The SslHandler is added to the pipeline by the SniHandler (replacing it) after the ClientHello.
        // It is added using the fully-qualified class name.
        SslHandler sslHandler = (SslHandler) this.clientCtx().pipeline().get(SslHandler.class.getName());
        return Optional.ofNullable(sslHandler)
                .map(SslHandler::engine)
                .map(SSLEngine::getSession)
                .orElse(null);
    }

    KafkaProxyFrontendHandler(
                              PluginFactoryRegistry pfr,
                              FilterChainFactory filterChainFactory,
                              List<NamedFilterDefinition> namedFilterDefinitions,
                              EndpointReconciler endpointReconciler,
                              ApiVersionsServiceImpl apiVersionsService,
                              DelegatingDecodePredicate dp,
                              TransportSubjectBuilder subjectBuilder,
                              EndpointBinding endpointBinding,
                              ClientSessionStateMachine sessionStateMachine) {
        this.endpointBinding = endpointBinding;
        this.pfr = pfr;
        this.filterChainFactory = filterChainFactory;
        this.namedFilterDefinitions = namedFilterDefinitions;
        this.endpointReconciler = endpointReconciler;
        this.apiVersionsIntersectFilter = new ApiVersionsIntersectFilter(apiVersionsService);
        this.apiVersionsDowngradeFilter = new ApiVersionsDowngradeFilter(apiVersionsService);
        this.dp = dp;
        this.subjectBuilder = Objects.requireNonNull(subjectBuilder);
        this.virtualClusterModel = endpointBinding.endpointGateway().virtualCluster();
        this.sessionStateMachine = sessionStateMachine;
        this.logNetwork = virtualClusterModel.isLogNetwork();
        this.logFrames = virtualClusterModel.isLogFrames();
        this.filters = null;
    }

    @Override
    public String toString() {
        return "KafkaProxyFrontendHandler{" +
                "clientCtx=" + clientCtx +
                ", sessionState=" + sessionStateMachine.currentStateName() +
                ", number of bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size()) +
                ", pendingClientFlushes=" + pendingClientFlushes +
                ", sniHostname='" + sniHostname + '\'' +
                ", pendingReadComplete=" + pendingReadComplete +
                ", blocked=" + progressionLatch +
                '}';
    }

    // ==================== Netty Channel Callbacks ====================

    /**
     * Netty callback. Used to notify us of custom events.
     * Events such as ServerNameIndicator (SNI) resolution completing.
     * <br>
     * This method is called for <em>every</em> custom event, so its up to us to filter out the ones we care about.
     *
     * @param ctx the channel handler context on which the event was triggered.
     * @param event the information being notified
     * @throws Exception any errors in processing.
     */
    @Override
    public void userEventTriggered(
                                   ChannelHandlerContext ctx,
                                   Object event)
            throws Exception {
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
     * Netty callback to notify that the downstream/client channel has an active TCP connection.
     * @param ctx the handler context for downstream/client channel
     * @throws Exception if we object to the client...
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientCtx = ctx;
        this.sessionStateMachine.onClientActive(this);
    }

    /**
     * Netty callback to notify that the downstream/client channel TCP connection has disconnected.
     * @param ctx The context for the downstream/client channel.
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
    public void channelWritabilityChanged(
                                          final ChannelHandlerContext ctx)
            throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            sessionStateMachine.onClientWritable();
        }
        else {
            sessionStateMachine.onClientUnwritable();
        }
    }

    /**
     * Netty callback that something has been read from the downstream/client channel.
     * @param ctx The context for the downstream/client channel.
     * @param msg the message read from the channel.
     */
    @Override
    public void channelRead(
                            ChannelHandlerContext ctx,
                            Object msg) {
        sessionStateMachine.onClientRequest(msg);
    }

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to apply backpressure to the <em>downstream/client</em> connection
     */
    void applyBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback from the {@link ProxyChannelStateMachine} triggered when it wants to remove backpressure from the <em>downstream/client</em> connection
     */
    void relieveBackpressure() {
        if (clientCtx != null) {
            this.clientCtx.channel().config().setAutoRead(true);
        }
    }

    // ==================== Callbacks from ClientSessionStateMachine ====================

    /**
     * Called on entry to ClientActive state.
     */
    public void inClientActive() {
        Channel clientChannel = clientCtx().channel();
        LOGGER.trace("{}: channelActive", clientChannel.id());
        this.clientSubjectManager = new ClientSubjectManager();
        this.progressionLatch = 2; // Require two events before unblocking

        if (!this.endpointBinding.endpointGateway().isUseTls()) {
            this.clientSubjectManager.subjectFromTransport(null, this.subjectBuilder, this::onTransportSubjectBuilt);
        }

        // install filters before first read
        addFiltersToPipeline(getFilters(), clientCtx().pipeline(), clientCtx().channel());
        // Initially the channel is not auto reading
        clientChannel.config().setAutoRead(false);
        clientChannel.read();
    }

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

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link SelectingServer} state.
     */
    void inSelectingServer() {
        var target = Objects.requireNonNull(endpointBinding.upstreamTarget());
        initiateConnect(target, getFilters());
        // NetFilter.selectServer() will callback on initiateConnect()
        // this.netFilter.selectServer(this, routingContext);
        this.sessionStateMachine.assertIsRouting(
                "NetFilter.selectServer() did not callback on NetFilterContext.initiateConnect(): filter=''");
    }

    @NonNull
    private List<FilterAndInvoker> getFilters() {
        if (this.filters != null) {
            return this.filters;
        }
        List<FilterAndInvoker> apiVersionFilters = FilterAndInvoker.build("ApiVersionsIntersect (internal)", apiVersionsIntersectFilter);
        var filterAndInvokers = new ArrayList<>(apiVersionFilters);
        filterAndInvokers.addAll(FilterAndInvoker.build("ApiVersionsDowngrade (internal)", apiVersionsDowngradeFilter));

        NettyFilterContext filterContext = new NettyFilterContext(clientCtx().channel().eventLoop(), pfr);
        List<FilterAndInvoker> filterChain = filterChainFactory.createFilters(filterContext, this.namedFilterDefinitions);
        filterAndInvokers.addAll(filterChain);

        if (endpointBinding.restrictUpstreamToMetadataDiscovery()) {
            filterAndInvokers.addAll(FilterAndInvoker.build("EagerMetadataLearner (internal)", new EagerMetadataLearner()));
        }
        filterAndInvokers.addAll(FilterAndInvoker.build("VirtualCluster TopicNameCache (internal)", virtualClusterModel.getTopicNameCacheFilter()));
        List<FilterAndInvoker> brokerAddressFilters = FilterAndInvoker.build("BrokerAddress (internal)",
                new BrokerAddressFilter(endpointBinding.endpointGateway(), endpointReconciler));
        filterAndInvokers.addAll(brokerAddressFilters);
        this.filters = filterAndInvokers;
        return filterAndInvokers;
    }

    /**
     * <p>Invoked when the last message read by the current read operation
     * has been consumed by {@link #channelRead(ChannelHandlerContext, Object)}.</p>
     * This allows the proxy to batch requests.
     * @param clientCtx The client context
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext clientCtx) {
        proxyChannelStateMachine.clientReadComplete();
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
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        proxyChannelStateMachine.onClientException(cause, endpointBinding.endpointGateway().getDownstreamSslContext().isPresent());
    }

    /**
     * Initiates the connection to a server.
     * Changes {@link #proxyChannelStateMachine} from {@link SelectingServer} to {@link Connecting}
     * Initializes the {@code backendHandler} and configures its pipeline
     * with the given {@code filters}.
     * @param remote upstream broker target
     * @param filters The protocol filters
     */
    void initiateConnect(
                         HostPort remote,
                         List<FilterAndInvoker> filters) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to backend broker {} using filters {}",
                    this.proxyChannelStateMachine.sessionId(), remote, filters);
        }
        this.proxyChannelStateMachine.onInitiateConnect(remote, filters, virtualClusterModel);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Connecting} state.
     */
    void inConnecting(
                      HostPort remote,
                      KafkaProxyBackendHandler backendHandler) {
        final Channel inboundChannel = clientCtx().channel();
        // Start the upstream connection attempt.
        final Bootstrap bootstrap = configureBootstrap(backendHandler, inboundChannel);

        LOGGER.debug("{}: Connecting to outbound {}", this.proxyChannelStateMachine.sessionId(), remote);
        ChannelFuture serverTcpConnectFuture = initConnection(remote.host(), remote.port(), bootstrap);
        Channel outboundChannel = serverTcpConnectFuture.channel();
        ChannelPipeline pipeline = outboundChannel.pipeline();

        var correlationManager = new CorrelationManager();

        // Note: Because we are acting as a client of the target cluster and are thus writing Request data to an outbound channel, the Request flows from the
        // last outbound handler in the pipeline to the first. When Responses are read from the cluster, the inbound handlers of the pipeline are invoked in
        // the reverse order, from first to last. This is the opposite of how we configure a server pipeline like we do in KafkaProxyInitializer where the channel
        // reads Kafka requests, as the message flows are reversed. This is also the opposite of the order that Filters are declared in the Kroxylicious configuration
        // file. The Netty Channel pipeline documentation provides an illustration https://netty.io/4.0/api/io/netty/channel/ChannelPipeline.html
        if (logFrames) {
            pipeline.addFirst("frameLogger", new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger", LogLevel.INFO));
        }
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
            channelReadComplete(this.clientCtx());
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
     * Called by the {@link ProxyChannelStateMachine} when there is a requirement to buffer RPC's prior to forwarding to the upstream/server.
     * Generally this is expected to be when client requests are received before we have a connection to the upstream node.
     * @param msg the RPC to buffer.
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
            // TODO configurable timeout
            // Handler name must be unique, but filters are allowed to appear multiple times
            String handlerName = "filter-" + ++num + "-" + protocolFilter.filterName();
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

    private void unblockClient() {
        var inboundChannel = clientCtx().channel();
        inboundChannel.config().setAutoRead(true);
        sessionStateMachine.onClientWritable();
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx, "clientCtx was null while in state " + this.sessionStateMachine.currentState());
    }

    private static ResponseFrame buildErrorResponseFrame(
                                                         DecodedRequestFrame<?> triggerFrame,
                                                         Throwable error) {
        var responseData = KafkaProxyExceptionMapper.errorResponseMessage(triggerFrame, error);
        final ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        responseHeaderData.setCorrelationId(triggerFrame.correlationId());
        return new DecodedResponseFrame<>(triggerFrame.apiVersion(), triggerFrame.correlationId(), responseHeaderData, responseData);
    }

    /**
     * Return an error response to send to the client, or null if no response should be sent.
     * @param errorCodeEx The exception
     * @return The response frame
     */
    private @Nullable ResponseFrame errorResponse(
                                                  @Nullable Throwable errorCodeEx) {
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

    /**
     * Get the ID of the frontend channel if available.
     * @return <code>null</code> if the channel is not yet available
     */
    @CheckReturnValue
    @Nullable
    public ChannelId channelId() {
        Channel channel = this.clientCtx != null ? this.clientCtx.channel() : null;
        return channel != null ? channel.id() : null;
    }

    protected String remoteHost() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getAddress().getHostAddress();
        }
        else {
            return String.valueOf(socketAddress);
        }
    }

    protected int remotePort() {
        SocketAddress socketAddress = clientCtx().channel().remoteAddress();
        if (socketAddress instanceof InetSocketAddress inetSocketAddress) {
            return inetSocketAddress.getPort();
        }
        else {
            return -1;
        }
    }

}

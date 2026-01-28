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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.haproxy.HAProxyMessage;
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
import io.kroxylicious.proxy.internal.codec.DecodePredicate;
import io.kroxylicious.proxy.internal.filter.ApiVersionsDowngradeFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsIntersectFilter;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter;
import io.kroxylicious.proxy.internal.filter.EagerMetadataLearner;
import io.kroxylicious.proxy.internal.filter.NettyFilterContext;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.UpstreamEndpoint;
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
 * <p>Works with {@link ProxyChannelStateMachine} for session state management
 * and {@link ProxyChannelStateMachine} for backend connection management.</p>
 */
public class KafkaProxyFrontendHandler
        extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyFrontendHandler.class);

    private final VirtualClusterModel virtualClusterModel;
    private final EndpointBinding endpointBinding;
    private final EndpointReconciler endpointReconciler;
    private final DelegatingDecodePredicate dp;
    private final ProxyChannelStateMachine proxyChannelStateMachine;
    private final TransportSubjectBuilder subjectBuilder;
    private final PluginFactoryRegistry pfr;
    private final FilterChainFactory filterChainFactory;
    private final List<NamedFilterDefinition> namedFilterDefinitions;
    private final ApiVersionsIntersectFilter apiVersionsIntersectFilter;
    private final ApiVersionsDowngradeFilter apiVersionsDowngradeFilter;

    private @Nullable ChannelHandlerContext clientCtx;
    @VisibleForTesting
    @Nullable
    List<Object> bufferedMsgs;
    private boolean pendingClientFlushes;
    private @Nullable String sniHostname;

    // Flag if we receive a channelReadComplete() prior to outbound connection activation
    // so we can perform the channelReadComplete()/outbound flush & auto_read
    // once the outbound channel is active
    private boolean pendingReadComplete = true;

    private @Nullable ClientSubjectManager clientSubjectManager;
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
                              ProxyChannelStateMachine proxyChannelStateMachine) {
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
        this.proxyChannelStateMachine = proxyChannelStateMachine;
    }

    @Override
    public String toString() {
        // Don't include proxyChannelStateMachine's toString here
        // because proxyChannelStateMachine's toString will include the frontend's toString,
        // and we don't want a SOE.
        return "KafkaProxyFrontendHandler{"
                + ", clientCtx=" + clientCtx
                + ", proxyChannelState=" + this.proxyChannelStateMachine.currentState()
                + ", number of bufferedMsgs=" + (bufferedMsgs == null ? 0 : bufferedMsgs.size())
                + ", pendingClientFlushes=" + pendingClientFlushes
                + ", sniHostname='" + sniHostname + '\''
                + ", pendingReadComplete=" + pendingReadComplete
                + ", blocked=" + progressionLatch
                + '}';
    }

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
        this.proxyChannelStateMachine.onClientActive(this);

    }

    /**
     * Netty callback to notify that the downstream/client channel TCP connection has disconnected.
     * @param ctx The context for the downstream/client channel.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.trace("INACTIVE on inbound {}", ctx.channel());
        proxyChannelStateMachine.onClientInactive();
    }

    /**
     * Propagates backpressure to the <em>upstream/server</em> connection by notifying the {@link ProxyChannelStateMachine} when the <em>downstream/client</em> connection
     * blocks or unblocks.
     * @param ctx the handler context for downstream/client channel
     * @throws Exception If something went wrong
     */
    @Override
    public void channelWritabilityChanged(
                                          final ChannelHandlerContext ctx)
            throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            this.proxyChannelStateMachine.onClientWritable();
        }
        else {
            this.proxyChannelStateMachine.onClientUnwritable();
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
        proxyChannelStateMachine.onClientRequest(msg);
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

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ProxyChannelState.ClientActive} state.
     */
    void inClientActive() {
        Channel clientChannel = clientCtx().channel();
        LOGGER.trace("{}: channelActive", clientChannel.id());
        this.clientSubjectManager = new ClientSubjectManager();
        this.progressionLatch = 2; // we require two events before unblocking
        if (!this.endpointBinding.endpointGateway().isUseTls()) {
            this.clientSubjectManager.subjectFromTransport(null, this.subjectBuilder, this::onTransportSubjectBuilt);
        }

        // install filters before first read
        List<FilterAndInvoker> filters = buildFilters();
        addFiltersToPipeline(filters, clientCtx().pipeline(), clientCtx().channel());
        // Set the decode predicate now that we have the filters
        dp.setDelegate(DecodePredicate.forFilters(filters));
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
        var upstreamServiceEndpoints = Objects.requireNonNull(endpointBinding.allUpstreamServiceEndpoints());
        initiateConnect(upstreamServiceEndpoints);
    }

    @NonNull
    private List<FilterAndInvoker> buildFilters() {
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
     * Handles an exception in downstream/client pipeline by notifying {@link #proxyChannelStateMachine} of the issue.
     * @param ctx The downstream context
     * @param cause The downstream exception
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
     * @param upstreamEndpoints The upstream endpoints to connect to.
     */
    void initiateConnect(List<UpstreamEndpoint> upstreamEndpoints) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("{}: Connecting to {} backend(s)",
                    this.proxyChannelStateMachine.sessionId(), upstreamEndpoints.size());
        }
        this.proxyChannelStateMachine.onInitiateConnect(clientCtx.channel(), upstreamEndpoints);
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Connecting} state.
     */
    void inConnecting() {
        LOGGER.debug("{}: inConnecting", proxyChannelStateMachine.sessionId());
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link Forwarding} state.
     */
    void inForwarding() {
        // connection is complete, so first forward the buffered message
        if (bufferedMsgs != null) {
            for (Object bufferedMsg : bufferedMsgs) {
                proxyChannelStateMachine.messageFromClient(bufferedMsg);
            }
            bufferedMsgs = null;
        }

        if (pendingReadComplete) {
            pendingReadComplete = false;
            channelReadComplete(this.clientCtx());
        }

        // once buffered message has been forwarded we enable auto-read to start accepting further messages
        onReadyToForwardMore();
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} on entry to the {@link ProxyChannelState.Closed} state.
     */
    void inClosed(@Nullable Throwable errorCodeEx) {
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

    /**
     * Called by the {@link ProxyChannelStateMachine} to propagate an RPC to the downstream client.
     * @param msg the RPC to forward.
     */
    void forwardToClient(Object msg) {
        final Channel inboundChannel = clientCtx().channel();
        if (inboundChannel.isWritable()) {
            inboundChannel.write(msg, clientCtx().voidPromise());
            pendingClientFlushes = true;
        }
        else {
            inboundChannel.writeAndFlush(msg, clientCtx().voidPromise());
            pendingClientFlushes = false;
        }
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} when the bach from the upstream/server side is complete.
     */
    void flushToClient() {
        final Channel inboundChannel = clientCtx().channel();
        if (pendingClientFlushes) {
            pendingClientFlushes = false;
            inboundChannel.flush();
        }
        if (!inboundChannel.isWritable()) {
            // TODO does duplicate the writeability change notification from netty? If it does is that a problem?
            proxyChannelStateMachine.onClientUnwritable();
        }
    }

    /**
     * Called by the {@link ProxyChannelStateMachine} when there is a requirement to buffer RPC's prior to forwarding to the upstream/server.
     * Generally this is expected to be when client requests are received before we have a connection to the upstream node.
     * @param msg the RPC to buffer.
     */
    void bufferMsg(Object msg) {
        if (msg instanceof HAProxyMessage) {
            // HAProxy messages are pre-kafka protocol and must be processed immediately
            // todo: should be release the HAProxyMessage now?
            return;
        }

        if (bufferedMsgs == null) {
            bufferedMsgs = new ArrayList<>();
        }
        bufferedMsgs.add(msg);
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
                            proxyChannelStateMachine,
                            clientSubjectManager));
        }
    }

    private void unblockClient() {
        var inboundChannel = clientCtx().channel();
        inboundChannel.config().setAutoRead(true);
        proxyChannelStateMachine.onClientWritable();
    }

    private ChannelHandlerContext clientCtx() {
        return Objects.requireNonNull(this.clientCtx, "clientCtx was null while in state " + this.proxyChannelStateMachine.currentState());
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

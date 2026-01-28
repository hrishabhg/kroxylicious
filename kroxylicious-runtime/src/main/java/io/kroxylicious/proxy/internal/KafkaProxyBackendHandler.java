/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Netty channel handler for backend/upstream server connections.
 *
 * <p>This handler manages all Netty I/O callbacks for connections to Kafka brokers:</p>
 * <ul>
 *   <li>Connection lifecycle (active, inactive)</li>
 *   <li>TLS handshake completion</li>
 *   <li>Message reading and forwarding to client</li>
 *   <li>Backpressure propagation via writability changes</li>
 *   <li>Exception handling</li>
 * </ul>
 *
 * <p>Works with {@link BackendStateMachine} which manages the connection state
 * and coordinates with {@link ProxyChannelStateMachine}
 * for multi-cluster scenarios.</p>
 */
public class KafkaProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyBackendHandler.class);

    @VisibleForTesting
    final BackendStateMachine backendStateMachine;

    @VisibleForTesting
    final @Nullable SslContext sslContext;

    @Nullable
    ChannelHandlerContext serverCtx;

    private boolean pendingServerFlushes;

    /**
     * Creates a new backend handler for the given state machine.
     *
     * @param backendStateMachine the state machine managing this connection
     * @param upstreamSslContext optional TLS context for upstream connection
     */
    public KafkaProxyBackendHandler(BackendStateMachine backendStateMachine, Optional<SslContext> upstreamSslContext) {
        this.backendStateMachine = Objects.requireNonNull(backendStateMachine);
        this.sslContext = upstreamSslContext.orElse(null);
    }

    /**
     * Propagates backpressure to the downstream/client connection by notifying the
     * {@link BackendStateMachine} when the upstream/server connection blocks or unblocks.
     */
    @Override
    public void channelWritabilityChanged(final ChannelHandlerContext ctx) throws Exception {
        super.channelWritabilityChanged(ctx);
        if (ctx.channel().isWritable()) {
            backendStateMachine.onServerWritable();
        }
        else {
            backendStateMachine.onServerUnwritable();
        }
    }

    /**
     * Netty callback that resources have been allocated for the channel.
     * This is the first point at which we become aware of the upstream/server channel.
     */
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        this.serverCtx = ctx;
        backendStateMachine.setServerContext(ctx);
        super.channelRegistered(ctx);
    }

    /**
     * Netty callback that upstream/server channel has successfully connected to the remote peer.
     * This does not mean that the channel is usable by the proxy as TLS negotiation,
     * if required, is still in progress.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.trace("{}: Channel active for cluster {}",
                backendStateMachine.sessionId(), backendStateMachine.clusterId());
        if (sslContext == null) {
            backendStateMachine.onConnectionActive();
        }
        super.channelActive(ctx);
    }

    /**
     * Netty callback for custom events like SSL Handshake completion.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
        if (event instanceof SslHandshakeCompletionEvent sslEvt) {
            if (sslEvt.isSuccess()) {
                backendStateMachine.onConnectionActive();
            }
            else {
                backendStateMachine.onConnectionFailed(sslEvt.cause());
            }
        }
        super.userEventTriggered(ctx, event);
    }

    /**
     * Netty callback that the upstream/server channel TCP connection has disconnected.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        backendStateMachine.onConnectionInactive();
    }

    /**
     * Netty callback indicating that an exception reached Netty.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        backendStateMachine.onConnectionError(cause);
    }

    /**
     * Netty callback that something has been read from the upstream/server channel.
     * There may be further calls to this method before a call to {@code channelReadComplete()}
     * signals the end of the current read operation.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        backendStateMachine.onMessageFromServer(msg);
    }

    /**
     * Invoked when the last message read by the current read operation has been consumed.
     * This allows the proxy to batch responses.
     */
    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        backendStateMachine.onServerReadComplete();
    }

    // ==================== Methods called by BackendStateMachine ====================

    /**
     * Called by the {@link BackendStateMachine} to propagate an RPC to the upstream node.
     *
     * @param msg the RPC to forward
     */
    public void forwardToServer(Object msg) {
        if (serverCtx == null) {
            backendStateMachine.illegalState("write without active outbound channel");
            return;
        }

        final Channel outboundChannel = serverCtx.channel();
        if (outboundChannel.isWritable()) {
            outboundChannel.write(msg, serverCtx.voidPromise());
            pendingServerFlushes = true;
        }
        else {
            outboundChannel.writeAndFlush(msg, serverCtx.voidPromise());
            pendingServerFlushes = false;
        }
        LOGGER.trace("{}: Forwarded to cluster {}", backendStateMachine.sessionId(), backendStateMachine.clusterId());
    }

    /**
     * Called by the {@link BackendStateMachine} when the batch from the downstream/client side is complete.
     */
    public void flushToServer() {
        if (serverCtx != null) {
            final Channel serverChannel = serverCtx.channel();
            if (pendingServerFlushes) {
                pendingServerFlushes = false;
                serverChannel.flush();
            }
            if (!serverChannel.isWritable()) {
                backendStateMachine.onServerUnwritable();
            }
        }
    }

    /**
     * Callback triggered when we want to apply backpressure to the upstream/server connection.
     */
    public void applyBackpressure() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(false);
        }
    }

    /**
     * Callback triggered when we want to remove backpressure from the upstream/server connection.
     */
    public void relieveBackpressure() {
        if (serverCtx != null) {
            serverCtx.channel().config().setAutoRead(true);
        }
    }

    /**
     * Called by the {@link BackendStateMachine} on entry to the Closed state.
     */
    public void inClosed() {
        if (serverCtx != null) {
            Channel outboundChannel = serverCtx.channel();
            if (outboundChannel.isActive()) {
                outboundChannel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                        .addListener(ChannelFutureListener.CLOSE);
            }
        }
    }

    /**
     * Check if the server channel is writable.
     *
     * @return true if writable, false otherwise
     */
    public boolean isChannelWritable() {
        return serverCtx != null && serverCtx.channel().isWritable();
    }

    @Override
    public String toString() {
        return "KafkaProxyBackendHandler{" +
                "clusterId=" + backendStateMachine.clusterId() +
                ", serverCtx=" + serverCtx +
                ", state=" + backendStateMachine.state() +
                ", pendingServerFlushes=" + pendingServerFlushes +
                '}';
    }
}

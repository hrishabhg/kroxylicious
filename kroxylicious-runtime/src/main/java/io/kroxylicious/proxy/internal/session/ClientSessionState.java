/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.session;

import org.apache.kafka.common.message.ApiVersionsRequestData;

import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing client session states.
 * These states are independent of backend connections - they represent
 * the client's progression through the Kafka protocol handshake.
 *
 * <pre>
 *   Startup
 *      │
 *      ▼
 *   ClientActive ──────────────────────────────┐
 *      │                                       │
 *      ▼ (PROXY header)                        │ (no PROXY)
 *   HaProxy                                    │
 *      │                                       │
 *      ├───────────────────────────────────────┤
 *      │                                       │
 *      ▼ (ApiVersions request)                 ▼ (other request)
 *   ApiVersions ────────────────────────────► Routing
 *                                               │
 *                                               ▼
 *                                            Closed
 * </pre>
 */
public sealed interface ClientSessionState permits
        ClientSessionState.Startup,
        ClientSessionState.ClientActive,
        ClientSessionState.HaProxy,
        ClientSessionState.ApiVersions,
        ClientSessionState.Routing, // aka: forwarding
        ClientSessionState.Closed {

    /**
     * Initial state - state machine just created.
     */
    record Startup() implements ClientSessionState {
        public static final Startup INSTANCE = new Startup();

        public ClientActive toClientActive() {
            return new ClientActive();
        }
    }

    /**
     * Client TCP connection is active, no messages received yet.
     */
    record ClientActive() implements ClientSessionState {

        public HaProxy toHaProxy(HAProxyMessage haProxyMessage) {
            return new HaProxy(haProxyMessage);
        }

        public ApiVersions toApiVersions(DecodedRequestFrame<ApiVersionsRequestData> frame) {
            return new ApiVersions(
                    null,
                    frame.body().clientSoftwareName(),
                    frame.body().clientSoftwareVersion());
        }

        public Routing toRouting(@Nullable DecodedRequestFrame<ApiVersionsRequestData> frame) {
            return new Routing(
                    null,
                    frame == null ? null : frame.body().clientSoftwareName(),
                    frame == null ? null : frame.body().clientSoftwareVersion());
        }
    }

    /**
     * PROXY protocol header received.
     */
    record HaProxy(HAProxyMessage haProxyMessage) implements ClientSessionState {

        public ApiVersions toApiVersions(DecodedRequestFrame<ApiVersionsRequestData> frame) {
            return new ApiVersions(
                    haProxyMessage,
                    frame.body().clientSoftwareName(),
                    frame.body().clientSoftwareVersion());
        }

        public Routing toRouting(@Nullable DecodedRequestFrame<ApiVersionsRequestData> frame) {
            return new Routing(
                    haProxyMessage,
                    frame == null ? null : frame.body().clientSoftwareName(),
                    frame == null ? null : frame.body().clientSoftwareVersion());
        }
    }

    /**
     * ApiVersions request received and responded to.
     */
    record ApiVersions(
                       @Nullable HAProxyMessage haProxyMessage,
                       @Nullable String clientSoftwareName,
                       @Nullable String clientSoftwareVersion)
            implements ClientSessionState {

        public Routing toRouting() {
            return new Routing(haProxyMessage, clientSoftwareName, clientSoftwareVersion);
        }
    }

    /**
     * Client is ready for routing - backend connections can be established.
     * This is the steady state during normal operation.
     */
    record Routing(
                   @Nullable HAProxyMessage haProxyMessage,
                   @Nullable String clientSoftwareName,
                   @Nullable String clientSoftwareVersion)
            implements ClientSessionState {

        public Closed toClosed() {
            return Closed.INSTANCE;
        }
    }

    /**
     * Terminal state - session is closed.
     */
    record Closed() implements ClientSessionState { public static final Closed INSTANCE = new Closed(); }
}
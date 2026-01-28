/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Optional;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing backend connection states.
 * Each cluster connection has its own independent state machine.
 *
 * <pre>
 *   Created
 *      │
 *      ▼ connect()
 *   Connecting
 *      │
 *      ├──────────────► Failed (connection error)
 *      │
 *      ▼ onActive()
 *   Connected
 *      │
 *      ▼ onInactive() or onError()
 *   Closed
 * </pre>
 */
public sealed interface BackendConnectionState permits
        BackendConnectionState.Created,
        BackendConnectionState.Connecting,
        BackendConnectionState.Connected,
        BackendConnectionState.Failed,
        BackendConnectionState.Closed {

    /**
     * Initial state - backend not yet connecting.
     */
    record Created() implements BackendConnectionState {
        public static final Created INSTANCE = new Created();

        public Connecting toConnecting(HostPort target, Optional<SslContext> sslContext) {
            return new Connecting(target, sslContext);
        }
    }

    /**
     * TCP connection in progress (and TLS handshake if applicable).
     */
    record Connecting(
                      HostPort target,
                      Optional<SslContext> sslContext)
            implements BackendConnectionState {

        public Connected toConnected() {
            return new Connected(target);
        }

        public Failed toFailed(Throwable cause) {
            return new Failed(target, cause);
        }
    }

    /**
     * Connection established and ready for KRPC.
     */
    record Connected(HostPort target) implements BackendConnectionState {

        public Closed toClosed() {
            return Closed.INSTANCE;
        }
    }

    /**
     * Connection attempt failed.
     */
    record Failed(
                  HostPort target,
                  @Nullable Throwable cause)
            implements BackendConnectionState {

        public Closed toClosed() {
            return Closed.INSTANCE;
        }
    }

    /**
     * Terminal state - connection closed.
     */
    record Closed() implements BackendConnectionState { public static final Closed INSTANCE = new Closed(); }

    // Convenience methods
    default boolean isConnected() {
        return this instanceof Connected;
    }

    default boolean isTerminal() {
        return this instanceof Closed || this instanceof Failed;
    }

    default boolean canSendRequests() {
        return this instanceof Connected;
    }
}
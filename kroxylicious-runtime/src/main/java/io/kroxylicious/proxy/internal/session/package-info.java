/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Session management for Kroxylicious proxy connections.
 *
 * <h2>Architecture</h2>
 *
 * <p>This package provides a clean separation between client session management
 * and backend cluster connections:</p>
 *
 * <pre>
 *                                                ┌─► BackendStateMachine₁ → Cluster₁
 * Client → FrontendHandler → ClientSessionSM ───┼─► BackendStateMachine₂ → Cluster₂
 *                                    │          └─► BackendStateMachine₃ → Cluster₃
 *                                    │
 *                                    └── ProxyChannelStateMachine
 * </pre>
 *
 * <h2>Key Components</h2>
 *
 * <dl>
 *   <dt>{@link io.kroxylicious.proxy.internal.session.ClientSessionState}</dt>
 *   <dd>Sealed hierarchy for client-side session states (Startup → ClientActive → ApiVersions → Routing)</dd>
 *
 *   <dt>{@link io.kroxylicious.proxy.internal.session.ClientSessionStateMachine}</dt>
 *   <dd>Manages client session lifecycle, independent of backend connections</dd>
 *
 *   <dt>{@link io.kroxylicious.proxy.internal.session.BackendConnectionState}</dt>
 *   <dd>Sealed hierarchy for per-cluster connection states (Created → Connecting → Connected → Closed)</dd>
 *
 *   <dt>{@link io.kroxylicious.proxy.internal.session.BackendStateMachine}</dt>
 *   <dd>Manages individual cluster connection lifecycle</dd>
 *
 *   <dt>{@link io.kroxylicious.proxy.ProxyChannelStateMachine}</dt>
 *   <dd>Aggregates multiple BackendStateMachines, provides routing and backpressure coordination</dd>
 * </dl>
 *
 * <h2>Single vs Multi-Cluster</h2>
 *
 * <p>For single-cluster deployments, ProxyChannelStateMachine contains exactly one BackendStateMachine.
 * For multi-cluster, it contains one per configured cluster. The API is identical in both cases.</p>
 *
 * <h2>Migration from ProxyChannelStateMachine</h2>
 *
 * <p>The original {@code ProxyChannelStateMachine} combined client session and backend connection
 * management. This package separates those concerns:</p>
 *
 * <table>
 *   <tr><th>Original</th><th>New Location</th></tr>
 *   <tr><td>Startup, ClientActive, HaProxy, ApiVersions states</td><td>ClientSessionState</td></tr>
 *   <tr><td>SelectingServer state</td><td>Routing state in ClientSessionState</td></tr>
 *   <tr><td>Connecting, Forwarding states</td><td>BackendConnectionState (per cluster)</td></tr>
 *   <tr><td>Client lifecycle methods</td><td>ClientSessionStateMachine</td></tr>
 *   <tr><td>Backend lifecycle methods</td><td>BackendStateMachine</td></tr>
 *   <tr><td>Backpressure management</td><td>Split: client→backend in ClientSessionSM, backend→client in BackendSM</td></tr>
 * </table>
 *
 * @see io.kroxylicious.proxy.internal.session.ClientSessionStateMachine
 * @see io.kroxylicious.proxy.ProxyChannelStateMachine
 * @see io.kroxylicious.proxy.internal.session.BackendStateMachine
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.internal.session;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
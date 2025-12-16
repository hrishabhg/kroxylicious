/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Multi-cluster routing infrastructure for Kroxylicious proxy.
 *
 * <p>This package provides the runtime implementations of the multi-cluster
 * routing API defined in {@code io.kroxylicious.proxy.net}:</p>
 *
 * <ul>
 *   <li>{@link io.kroxylicious.proxy.internal.net.RoutingContextImpl} -
 *       Implementation of {@link io.kroxylicious.proxy.net.RoutingContext}
 *       that bridges filters to the {@link io.kroxylicious.proxy.internal.session.ClusterConnectionManager}</li>
 *
 *   <li>{@link io.kroxylicious.proxy.internal.net.ClusterRouteImpl} -
 *       Implementation of {@link io.kroxylicious.proxy.net.ClusterRoute}
 *       that wraps a {@link io.kroxylicious.proxy.internal.session.BackendStateMachine}</li>
 * </ul>
 *
 * <h2>Architecture Overview</h2>
 *
 * <pre>
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │                    FILTER LAYER (API)                        │
 *   │  FilterContext.routingContext() → RoutingContext             │
 *   │                       ↓                                      │
 *   │  RoutingContext.cluster("id") → ClusterRoute                 │
 *   │                       ↓                                      │
 *   │  ClusterRoute.sendRequest() → CompletionStage&lt;Response&gt;      │
 *   └──────────────────────────────────────────────────────────────┘
 *                           │
 *                           ▼
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │                  RUNTIME LAYER (this package)                │
 *   │  RoutingContextImpl wraps ClusterConnectionManager           │
 *   │  ClusterRouteImpl wraps BackendStateMachine                  │
 *   └──────────────────────────────────────────────────────────────┘
 *                           │
 *                           ▼
 *   ┌──────────────────────────────────────────────────────────────┐
 *   │                  SESSION LAYER                               │
 *   │  ClusterConnectionManager manages BackendStateMachine(s)     │
 *   │  BackendStateMachine manages Netty channel per cluster       │
 *   └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Key Concepts</h2>
 *
 * <h3>NodeId Offsets</h3>
 * <p>Each cluster is assigned a nodeId offset (0, 1000, 2000, ...) to ensure
 * unique nodeIds when merging metadata from multiple clusters. Filters use
 * {@link io.kroxylicious.proxy.net.RoutingContext#applyNodeIdOffset(int, String)}
 * and {@link io.kroxylicious.proxy.net.RoutingContext#removeNodeIdOffset(int, String)}
 * to translate between cluster-local and global nodeIds.</p>
 *
 * <h3>Correlation ID Management</h3>
 * <p>Async requests initiated via {@link io.kroxylicious.proxy.net.ClusterRoute#sendRequest}
 * use negative correlation IDs to distinguish them from client-initiated requests.
 * This ensures responses are routed back to the requesting filter rather than
 * being forwarded to the client.</p>
 *
 * @see io.kroxylicious.proxy.net.RoutingContext
 * @see io.kroxylicious.proxy.net.ClusterRoute
 * @see io.kroxylicious.proxy.internal.session.ClusterConnectionManager
 * @see io.kroxylicious.proxy.internal.session.BackendStateMachine
 */

@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.internal.net;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;
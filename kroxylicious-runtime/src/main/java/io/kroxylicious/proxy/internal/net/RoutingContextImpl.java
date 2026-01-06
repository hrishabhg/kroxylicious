/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.net;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.session.BackendStateMachine;
import io.kroxylicious.proxy.internal.session.ClusterConnectionManager;
import io.kroxylicious.proxy.net.ClusterRoute;
import io.kroxylicious.proxy.net.RoutingContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Implementation of {@link RoutingContext} that wraps a {@link ClusterConnectionManager}.
 *
 * <p>This class provides filters with the ability to:</p>
 * <ul>
 *   <li>Discover available clusters</li>
 *   <li>Send requests to specific clusters</li>
 *   <li>Fan-out requests to multiple clusters</li>
 * </ul>
 */
public class RoutingContextImpl implements RoutingContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingContextImpl.class);

    private final ClusterConnectionManager connectionManager;
    private final Map<String, ClusterRouteImpl> routeCache = new HashMap<>();

    /**
     * Create a multi-cluster routing context.
     *
     * @param connectionManager the connection manager (maybe null for single-cluster backward compat)
     */
    public RoutingContextImpl(@Nullable ClusterConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public boolean isMultiCluster() {
        return connectionManager != null && connectionManager.isMultiCluster();
    }

    @Override
    public Set<String> clusterIds() {
        if (connectionManager == null) {
            return Set.of();
        }
        return connectionManager.clusterIds();
    }

    @Override
    public Optional<ClusterRoute> cluster(String clusterId) {
        if (connectionManager == null) {
            return Optional.empty();
        }

        BackendStateMachine backend = connectionManager.getBackend(clusterId);
        if (backend == null) {
            return Optional.empty();
        }

        return Optional.of(routeCache.computeIfAbsent(clusterId,
                id -> new ClusterRouteImpl(backend)));
    }

    @Override
    public ClusterRoute primaryCluster() {
        if (connectionManager == null) {
            throw new IllegalStateException("No connection manager available");
        }

        String primaryId = connectionManager.primaryClusterId();
        return cluster(primaryId).orElseThrow(() -> new IllegalStateException("Primary cluster not found: " + primaryId));
    }

    @Override
    public String primaryClusterId() {
        if (connectionManager == null) {
            throw new IllegalStateException("No connection manager available");
        }
        return connectionManager.primaryClusterId();
    }

    @Override
    public boolean isAnyConnected() {
        return connectionManager != null && connectionManager.isAnyConnected();
    }

    @Override
    public boolean areAllConnected() {
        return connectionManager != null && connectionManager.areAllConnected();
    }

    @Override
    public <M extends ApiMessage> CompletionStage<M> sendRequest(
                                                                 String clusterId,
                                                                 RequestHeaderData header,
                                                                 ApiMessage request) {

        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(request, "request");

        ClusterRoute route = cluster(clusterId).orElseThrow(() -> new IllegalArgumentException("Unknown cluster: " + clusterId));

        if (!route.isConnected()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cluster not connected: " + clusterId));
        }

        LOGGER.debug("Sending request to cluster {}: {}", clusterId, request.getClass().getSimpleName());
        return route.sendRequest(header, request);
    }

    @Override
    public <M extends ApiMessage> CompletionStage<Map<String, M>> fanOutRequest(
                                                                                Set<String> clusterIds,
                                                                                RequestHeaderData header,
                                                                                ApiMessage request) {

        Objects.requireNonNull(clusterIds, "clusterIds");
        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(request, "request");

        if (clusterIds.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        LOGGER.debug("Fan-out request to {} clusters: {}",
                clusterIds.size(), request.getClass().getSimpleName());

        // Create a future for each cluster
        Map<String, CompletableFuture<M>> futures = new HashMap<>();

        for (String clusterId : clusterIds) {
            Optional<ClusterRoute> routeOpt = cluster(clusterId);
            if (routeOpt.isEmpty()) {
                LOGGER.warn("Skipping unknown cluster in fan-out: {}", clusterId);
                continue;
            }

            ClusterRoute route = routeOpt.get();
            if (!route.isConnected()) {
                LOGGER.warn("Skipping disconnected cluster in fan-out: {}", clusterId);
                continue;
            }

            // Clone header for each request to avoid correlation ID conflicts
            RequestHeaderData clonedHeader = cloneHeader(header);

            CompletableFuture<M> future = route.<M> sendRequest(clonedHeader, request)
                    .toCompletableFuture();
            futures.put(clusterId, future);
        }

        if (futures.isEmpty()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("No connected clusters available for fan-out"));
        }

        // Wait for all futures and collect results
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
                futures.values().toArray(new CompletableFuture[0]));

        return allOf.thenApply(v -> {
            Map<String, M> results = new HashMap<>();
            for (var entry : futures.entrySet()) {
                try {
                    M response = entry.getValue().join();
                    results.put(entry.getKey(), response);
                }
                catch (Exception e) {
                    LOGGER.warn("Fan-out request failed for cluster {}: {}",
                            entry.getKey(), e.getMessage());
                    // Skip failed responses - caller can check which clusters responded
                }
            }
            return results;
        });
    }

    @Override
    public <M extends ApiMessage> CompletionStage<Map<String, M>> fanOutRequestToAll(
                                                                                     RequestHeaderData header,
                                                                                     ApiMessage request) {
        return fanOutRequest(clusterIds(), header, request);
    }

    /**
     * Clone a request header for fan-out (each cluster needs its own correlation ID).
     */
    private RequestHeaderData cloneHeader(RequestHeaderData original) {
        return new RequestHeaderData()
                .setRequestApiKey(original.requestApiKey())
                .setRequestApiVersion(original.requestApiVersion())
                .setClientId(original.clientId())
                .setCorrelationId(-1); // Will be assigned by framework
    }

    @Override
    public String toString() {
        return "MultiClusterRoutingContextImpl{" +
                "clusters=" + clusterIds() +
                ", anyConnected=" + isAnyConnected() +
                '}';
    }
}

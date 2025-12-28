/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.net;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Provides multi-cluster routing capabilities to filters.
 *
 * <p>This context allows filters to:</p>
 * <ul>
 *   <li>Discover available clusters</li>
 *   <li>Send requests to specific clusters</li>
 *   <li>Fan-out requests to multiple clusters</li>
 *   <li>Access cluster metadata (nodeId offsets) for response merging</li>
 * </ul>
 *
 * <p>For single-cluster deployments, this context contains exactly one cluster route
 * accessible via {@link #primaryCluster()}.</p>
 *
 * <h2>Example: Metadata Fan-out</h2>
 * <pre>{@code
 * MultiClusterRoutingContext routing = context.routingContext();
 *
 * if (routing.isMultiCluster()) {
 *     // Fan out to all clusters
 *     routing.fanOutRequestToAll(header, request)
 *         .thenApply(responses -> mergeMetadata(responses, routing));
 * } else {
 *     // Single cluster - forward normally
 *     context.forwardRequest(header, request);
 * }
 * }</pre>
 *
 * <h2>Example: Coordinator Routing</h2>
 * <pre>{@code
 * String targetCluster = getClusterForGroup(groupId);
 * routing.sendRequest(targetCluster, header, request)
 *     .thenApply(response -> adjustNodeIds(response, routing.cluster(targetCluster)));
 * }</pre>
 */
public interface RoutingContext {

    /**
     * Whether this context has multiple clusters configured.
     *
     * @return true if more than one cluster is available
     */
    boolean isMultiCluster();

    /**
     * Get all available cluster identifiers.
     *
     * @return set of cluster IDs
     */
    Set<String> clusterIds();

    /**
     * Get a specific cluster route by ID.
     *
     * @param clusterId the cluster identifier
     * @return the cluster route, or empty if not found
     */
    Optional<ClusterRoute> cluster(String clusterId);

    /**
     * Get the primary (default) cluster route.
     *
     * <p>For single-cluster deployments, this is the only cluster.
     * For multi-cluster, this is typically the first configured cluster
     * or one explicitly marked as primary.</p>
     *
     * @return the primary cluster route
     */
    ClusterRoute primaryCluster();

    /**
     * Get the primary cluster's identifier.
     *
     * @return primary cluster ID
     */
    String primaryClusterId();

    /**
     * Check if any cluster is connected.
     *
     * @return true if at least one cluster is connected
     */
    boolean isAnyConnected();

    /**
     * Check if all clusters are connected.
     *
     * @return true if all clusters are connected
     */
    boolean areAllConnected();

    /**
     * Send a request to a specific cluster.
     *
     * @param <M> response message type
     * @param clusterId target cluster identifier
     * @param header request header
     * @param request request message
     * @return CompletionStage completing with the response
     * @throws IllegalArgumentException if clusterId is unknown
     * @throws IllegalStateException if cluster is not connected
     */
    <M extends ApiMessage> CompletionStage<M> sendRequest(
            String clusterId,
            RequestHeaderData header,
            ApiMessage request);

    /**
     * Fan-out a request to specified clusters and collect responses.
     *
     * <p>The request is sent to each specified cluster concurrently.
     * The returned CompletionStage completes when all clusters respond.</p>
     *
     * @param <M> response message type
     * @param clusterIds set of target cluster identifiers
     * @param header request header (will be cloned for each cluster)
     * @param request request message
     * @return CompletionStage completing with map of clusterId to response
     */
    <M extends ApiMessage> CompletionStage<Map<String, M>> fanOutRequest(
            Set<String> clusterIds,
            RequestHeaderData header,
            ApiMessage request);

    /**
     * Fan-out a request to all available clusters.
     *
     * <p>Equivalent to {@code fanOutRequest(clusterIds(), header, request)}.</p>
     *
     * @param <M> response message type
     * @param header request header
     * @param request request message
     * @return CompletionStage completing with map of clusterId to response
     */
    <M extends ApiMessage> CompletionStage<Map<String, M>> fanOutRequestToAll(
            RequestHeaderData header,
            ApiMessage request);

    // ==================== Node ID Utilities ====================

    /**
     * Apply a cluster's nodeId offset to an original nodeId.
     *
     * <p>Use when merging metadata from multiple clusters to ensure
     * unique nodeIds across all clusters.</p>
     *
     * @param nodeId original nodeId from cluster
     * @param clusterId cluster identifier
     * @return adjusted nodeId with cluster offset applied
     */
    default int applyNodeIdOffset(int nodeId, String clusterId) {
        return cluster(clusterId)
                .map(route -> nodeId + route.nodeIdOffset())
                .orElse(nodeId);
    }

    /**
     * Remove a cluster's nodeId offset to get the original nodeId.
     *
     * <p>Use when routing requests to a specific cluster based on
     * a nodeId that was previously offset.</p>
     *
     * @param adjustedNodeId nodeId with offset applied
     * @param clusterId cluster identifier
     * @return original nodeId without offset
     */
    default int removeNodeIdOffset(int adjustedNodeId, String clusterId) {
        return cluster(clusterId)
                .map(route -> adjustedNodeId - route.nodeIdOffset())
                .orElse(adjustedNodeId);
    }

    /**
     * Determine which cluster a nodeId belongs to based on offset ranges.
     *
     * <p>Assumes each cluster has a non-overlapping nodeId range based on offsets.</p>
     *
     * @param adjustedNodeId nodeId with offset applied
     * @return clusterId that owns this nodeId, or empty if not determinable
     */
    default Optional<String> clusterForNodeId(int adjustedNodeId) {
        for (String clusterId : clusterIds()) {
            Optional<ClusterRoute> route = cluster(clusterId);
            if (route.isPresent()) {
                int offset = route.get().nodeIdOffset();
                // Assuming 1000 nodeIds per cluster range
                if (adjustedNodeId >= offset && adjustedNodeId < offset + 1000) {
                    return Optional.of(clusterId);
                }
            }
        }
        return Optional.empty();
    }
}
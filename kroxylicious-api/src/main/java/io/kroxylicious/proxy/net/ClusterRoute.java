/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.net;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.tls.Tls;

/**
 * Represents a route to a backend Kafka cluster.
 *
 * <p>In multi-cluster configurations, each cluster has its own route with
 * independent connection state and node ID offset for metadata merging.</p>
 *
 * <p>This is an SPI interface - implementations are provided by the runtime.</p>
 */
public interface ClusterRoute {

    /**
     * Unique identifier for this cluster route.
     *
     * @return cluster identifier (e.g., "cluster1", "us-east", "primary")
     */
    String clusterId();

    /**
     * Bootstrap servers for this cluster.
     *
     * @return bootstrap servers string (e.g., "broker1:9092,broker2:9092")
     */
    String bootstrapServers();

    /**
     * TLS configuration for connecting to this cluster.
     *
     * @return TLS config, or empty if plaintext connection
     */
    Optional<Tls> tls();

    /**
     * Node ID offset used when merging metadata from multiple clusters.
     *
     * <p>Each cluster should have a unique offset range to avoid nodeId collisions
     * when presenting a unified view to clients. For example:</p>
     * <ul>
     *   <li>cluster1: offset 0 → nodeIds 0-999</li>
     *   <li>cluster2: offset 1000 → nodeIds 1000-1999</li>
     *   <li>cluster3: offset 2000 → nodeIds 2000-2999</li>
     * </ul>
     *
     * @return the offset to add to nodeIds from this cluster
     */
    int nodeIdOffset();

    /**
     * Whether this cluster route has an active connection.
     *
     * @return true if connected and ready for requests
     */
    boolean isConnected();

    /**
     * Send a request to this specific cluster and receive the response.
     *
     * <p>The request passes through filters upstream of the invoking filter.
     * The response similarly passes through upstream filters but not the invoking filter.</p>
     *
     * <h4>Header</h4>
     * <p>The caller should specify the {@link RequestHeaderData#requestApiVersion()}.
     * Kroxylicious will automatically set the {@link RequestHeaderData#requestApiKey()}
     * to match the request type. {@link RequestHeaderData#correlationId()} is managed internally.</p>
     *
     * <h4>Thread Safety</h4>
     * <p>Computation stages chained to the returned {@link CompletionStage} are guaranteed
     * to execute on the thread associated with the connection.</p>
     *
     * @param <M> The response message type
     * @param header The request header (requestApiVersion should be set)
     * @param request The request message
     * @return CompletionStage that completes with the response, or null for ack-less Produce
     */
    <M extends ApiMessage> CompletionStage<M> sendRequest(
                                                          RequestHeaderData header,
                                                          ApiMessage request);
}
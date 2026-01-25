/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.router;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.internal.net.BootstrapEndpointBinding;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.ServiceEndpoint;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Router decides which cluster(s) to route requests to.
 *
 * <p>Router is stateless configuration, created at bootstrap time.
 * It makes routing decisions based on:</p>
 * <ul>
 *   <li>Request ApiKey (Metadata, Produce, Fetch, etc.)</li>
 *   <li>Request content (topic name, consumer group, etc.)</li>
 *   <li>Configured routing rules</li>
 * </ul>
 */
public interface Router {

    EndpointBinding bootstrapEndpointBinding(EndpointGateway endpointGateway);

    /**
     * Get broker endpoint binding for given nodeId and target cluster.
     * @param endpointGateway gateway
     * @param nodeId nodeId in the target cluster
     * @param hostPort hostPort of the broker
     * @param targetCluster associated target cluster
     * @return broker endpoint binding
     */
    EndpointBinding brokerEndpointBinding(EndpointGateway endpointGateway, int nodeId, HostPort hostPort, TargetCluster targetCluster);

    EndpointBinding metadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, int nodeId);

    /**
     * Get response aggregator factory for an ApiKey.
     * Returns empty if this ApiKey doesn't require aggregation.
     */
    @Nullable
    <T extends ApiMessage> ApiMessageAggregator<T> aggregator(ApiKeys apiKey);
}
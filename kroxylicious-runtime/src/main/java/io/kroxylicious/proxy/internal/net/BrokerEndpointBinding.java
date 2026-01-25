/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.service.ServiceEndpoint;

/**
 * A broker specific endpoint binding.
 *
 * @param endpointGateway the endpoint listener
 * @param allUpstreamServiceEndpoints the upstream target of this binding
 * @param nodeId kafka nodeId of the target broker
 */
public record BrokerEndpointBinding(EndpointGateway endpointGateway, List<ServiceEndpoint> allUpstreamServiceEndpoints, Integer nodeId)
        implements NodeSpecificEndpointBinding {
    public BrokerEndpointBinding {
        Objects.requireNonNull(endpointGateway, "endpointGateway must not be null");
        Objects.requireNonNull(allUpstreamServiceEndpoints, "upstreamTargets must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    @Override
    public List<ServiceEndpoint> upstreamServiceEndpoints(ApiKeys apiKey) {
        if (apiKey == ApiKeys.METADATA) {
            return endpointGateway.targetClusters().stream().map(t -> new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t)).toList();
        }
        return allUpstreamServiceEndpoints;
    }

    @Override
    public Integer nodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "BrokerEndpointBinding[" +
                "endpointGateway=" + this.endpointGateway() + ", " +
                "upstreamTargets=" + this.allUpstreamServiceEndpoints() + ", " +
                "restrictUpstreamToMetadataDiscovery=" + this.restrictUpstreamToMetadataDiscovery() + ", " +
                "nodeId=" + nodeId + ']';
    }
}

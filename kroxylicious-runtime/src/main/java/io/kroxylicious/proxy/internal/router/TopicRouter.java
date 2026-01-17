/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.net.BootstrapEndpointBinding;
import io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.router.aggregator.ApiVersionResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.MetadataResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.SaslAuthenticateResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.SaslHandshakeResponseAggregator;

import edu.umd.cs.findbugs.annotations.Nullable;

public class TopicRouter implements Router {

    private final Map<ApiKeys, ApiMessageAggregator<?>> aggregators;

    public TopicRouter() {
        this.aggregators = new HashMap<>();
        this.aggregators.put(ApiKeys.API_VERSIONS, new ApiVersionResponseAggregator());
        this.aggregators.put(ApiKeys.SASL_HANDSHAKE, new SaslHandshakeResponseAggregator());
        this.aggregators.put(ApiKeys.SASL_AUTHENTICATE, new SaslAuthenticateResponseAggregator());
        this.aggregators.put(ApiKeys.METADATA, new MetadataResponseAggregator());
    }

    private static final Map<String, TargetCluster> TopicRoutes = Map.of(
            "alice", new TargetCluster("localhost:39092", Optional.empty()),
            "bob", new TargetCluster("localhost:62496", Optional.empty()),
            "default", new TargetCluster("localhost:39092", Optional.empty()));

    @Override
    public BootstrapEndpointBinding createBootstrapBinding() {
        return null;
    }

    @Override
    public BrokerEndpointBinding createBrokerBinding() {
        return null;
    }

    @Override
    public MetadataDiscoveryBrokerEndpointBinding createMetadataDiscoveryBinding() {
        return null;
    }

    @Override
    public @Nullable <T extends ApiMessage> ApiMessageAggregator<T> aggregator(ApiKeys apiKey) {
        if (aggregators.containsKey(apiKey)) {
            @SuppressWarnings("unchecked")
            ApiMessageAggregator<T> aggregator = (ApiMessageAggregator<T>) aggregators.get(apiKey);
            return aggregator;
        }

        return null; // good for no aggregation
    }
}

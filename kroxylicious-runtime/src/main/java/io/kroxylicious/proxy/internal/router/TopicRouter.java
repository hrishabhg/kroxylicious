/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.ApiVersionResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.MetadataResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.SaslAuthenticateResponseAggregator;
import io.kroxylicious.proxy.internal.router.aggregator.SaslHandshakeResponseAggregator;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.ServiceEndpoint;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class TopicRouter implements Router {

    private final Map<ApiKeys, ApiMessageAggregator<?>> aggregators;
    private final VirtualClusterModel virtualCluster;

    public TopicRouter(VirtualClusterModel virtualCluster) {
        this.aggregators = new HashMap<>();
        this.aggregators.put(ApiKeys.API_VERSIONS, new ApiVersionResponseAggregator());
        this.aggregators.put(ApiKeys.SASL_HANDSHAKE, new SaslHandshakeResponseAggregator());
        this.aggregators.put(ApiKeys.SASL_AUTHENTICATE, new SaslAuthenticateResponseAggregator());
        this.aggregators.put(ApiKeys.METADATA, new MetadataResponseAggregator());
        this.virtualCluster = virtualCluster;
    }

    private static final Map<String, TargetCluster> TopicRoutes = Map.of(
            "alice", new TargetCluster("localhost:39092", Optional.empty()),
            "bob", new TargetCluster("localhost:62496", Optional.empty()),
            "default", new TargetCluster("localhost:39092", Optional.empty()));

    // list of APIs that are always topic-aware. Metadata is not always topic-aware.
    private static Set<ApiKeys> TOPIC_AWARE_APIS = Set.of(
            ApiKeys.PRODUCE,
            ApiKeys.FETCH
    );

    private static int NODE_ID_OFFSET = 10000;

    // todo: should I cache the EndpointBinding instances?
    @Override
    public EndpointBinding bootstrapEndpointBinding(EndpointGateway endpointGateway) {
        return new EndpointBinding() {
            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return endpointGateway;
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> upstreamServiceEndpoints(@NonNull ApiKeys apiKey) {
                if (TOPIC_AWARE_APIS.contains(apiKey)) {
                    throw new IllegalArgumentException("API key " + apiKey + " not supported for bootstrap endpoint binding");
                }
                return allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> allUpstreamServiceEndpoints() {
                return TopicRoutes.values().stream()
                        .map(t -> new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t))
                        .toList();
            }

            @Nullable
            @Override
            public Integer nodeId() {
                return 0;
            }
        };
    }

    /**
     * @param endpointGateway gateway
     * @param nodeId returned by metadata response or other responses
     * @param hostPort hostPort of the broker
     * @param targetCluster associated target cluster
     * @return EndpointBinding representing the node specific binding
     */
    @Override
    public EndpointBinding brokerEndpointBinding(EndpointGateway endpointGateway, int nodeId, HostPort hostPort, TargetCluster targetCluster) {
        return new EndpointBinding() {

            private ServiceEndpoint topicAwareEndpoint;

            private List<ServiceEndpoint> allUpstreamServiceEndpoints;

            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return endpointGateway;
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> upstreamServiceEndpoints(ApiKeys apiKey) {
                ensureInitialized();
                if (TOPIC_AWARE_APIS.contains(apiKey)) {
                    if (topicAwareEndpoint == null) {
                        throw new IllegalStateException("Topic-aware endpoint not initialised for nodeId " + nodeId);
                    }
                    return List.of(topicAwareEndpoint);
                }
                return allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> allUpstreamServiceEndpoints() {
                ensureInitialized();
                return allUpstreamServiceEndpoints;
            }

            @Nullable
            @Override
            public Integer nodeId() {
                return nodeId + targetCluster.index() * NODE_ID_OFFSET;
            }

            private void ensureInitialized() {
                if (allUpstreamServiceEndpoints == null) {
                    allUpstreamServiceEndpoints = virtualCluster.targetClusters().stream().map(
                            t -> {
                                if (t.equals(targetCluster)) {
                                    topicAwareEndpoint = new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t);
                                    return topicAwareEndpoint;
                                }
                                else {
                                    return new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t);
                                }
                            }
                    ).toList();
                }
            }
        };
    }

    @Override
    public EndpointBinding metadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, int nodeId) {
        return new EndpointBinding() {
            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return endpointGateway;
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> upstreamServiceEndpoints(@NonNull ApiKeys apiKey) {
                return allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> allUpstreamServiceEndpoints() {
                return TopicRoutes.values().stream()
                        .map(t -> new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t))
                        .toList();
            }

            @Nullable
            @Override
            public Integer nodeId() {
                return nodeId;
            }

            @Override
            public boolean restrictUpstreamToMetadataDiscovery() {
                return true;
            }
        };
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

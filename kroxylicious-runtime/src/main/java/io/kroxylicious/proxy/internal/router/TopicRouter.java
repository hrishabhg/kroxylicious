/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.net.BootstrapEndpointBinding;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding;
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
        // describeCluster could be added here in future if needed
        // brokerAddressFilter
        this.virtualCluster = virtualCluster;
    }

    // list of APIs that are always topic-aware. Metadata is not always topic-aware.
    private static final Set<ApiKeys> TOPIC_AWARE_APIS = Set.of(
            ApiKeys.PRODUCE,
            ApiKeys.FETCH
    );

    // this call should go to any broker of first target cluster
    private static final Set<ApiKeys> COORDINATOR_APIS = Set.of(
            ApiKeys.FIND_COORDINATOR,
            ApiKeys.INIT_PRODUCER_ID
    );

    private static final Set<ApiKeys> TXN_APIS = Set.of(
            ApiKeys.TXN_OFFSET_COMMIT,
            ApiKeys.ADD_PARTITIONS_TO_TXN,
            ApiKeys.END_TXN,
            ApiKeys.WRITE_TXN_MARKERS
    );

    private static final int NODE_ID_OFFSET = 10000;

    public TargetCluster coordinatorTargetCluster() {
        return virtualCluster.targetClusters().get(0);
    }

    // todo: should I cache the EndpointBinding instances?
    @Override
    public BootstrapEndpointBinding bootstrapEndpointBinding(EndpointGateway endpointGateway) {
        return new BootstrapEndpointBinding() {
            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return endpointGateway;
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> upstreamServiceEndpoints(@NonNull ApiKeys apiKey) {
                if (TOPIC_AWARE_APIS.contains(apiKey) || TXN_APIS.contains(apiKey)) {
                    throw new IllegalArgumentException("API key " + apiKey + " not supported for bootstrap endpoint binding");
                }
                if (COORDINATOR_APIS.contains(apiKey)) {
                    TargetCluster t = coordinatorTargetCluster();
                    return List.of(new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t));
                }
                return allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> allUpstreamServiceEndpoints() {
                return virtualCluster.targetClusters().stream()
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
    public BrokerEndpointBinding brokerEndpointBinding(EndpointGateway endpointGateway, int nodeId, HostPort hostPort, TargetCluster targetCluster) {
        return new BrokerEndpointBinding() {

            private ServiceEndpoint topicAwareEndpoint;

            private ServiceEndpoint coordinatorEndpoint;

            private List<ServiceEndpoint> allUpstreamServiceEndpoints;

            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return endpointGateway;
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> upstreamServiceEndpoints(@NonNull ApiKeys apiKey) {
                if (TXN_APIS.contains(apiKey)) {
                    throw new IllegalArgumentException("API key " + apiKey + " not supported for broker endpoint binding");
                }
                ensureInitialized();
                if (TOPIC_AWARE_APIS.contains(apiKey)) {
                    if (topicAwareEndpoint == null) {
                        throw new IllegalStateException("Topic-aware endpoint not initialised for nodeId " + nodeId);
                    }
                    return List.of(topicAwareEndpoint);
                }

                if (COORDINATOR_APIS.contains(apiKey)) {
                    if (coordinatorEndpoint == null) {
                        throw new IllegalStateException("Coordinator endpoint not initialised for nodeId " + nodeId);
                    }
                    return List.of(coordinatorEndpoint);
                }

                return allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public List<ServiceEndpoint> allUpstreamServiceEndpoints() {
                ensureInitialized();
                return allUpstreamServiceEndpoints;
            }

            @NonNull
            @Override
            public Integer nodeId() {
                return nodeId + targetCluster.index() * NODE_ID_OFFSET;
            }

            private void ensureInitialized() {
                if (allUpstreamServiceEndpoints == null) {
                    allUpstreamServiceEndpoints = virtualCluster.targetClusters().stream().map(
                            t -> {
                                ServiceEndpoint endpoint;
                                if (t.equals(targetCluster)) {
                                    endpoint = new ServiceEndpoint(hostPort.host(), hostPort.port(), t);
                                    topicAwareEndpoint = endpoint;
                                }
                                else {
                                    endpoint = new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t);
                                }

                                if (coordinatorEndpoint == null && t.equals(coordinatorTargetCluster())) {
                                    coordinatorEndpoint = endpoint;
                                }

                                return endpoint;
                            }
                    ).toList();
                }
            }
        };
    }

    @Override
    public MetadataDiscoveryBrokerEndpointBinding metadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, int nodeId) {
        return new MetadataDiscoveryBrokerEndpointBinding() {
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
                return virtualCluster.targetClusters().stream()
                        .map(t -> new ServiceEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t))
                        .toList();
            }

            @Nullable
            @Override
            public Integer nodeId() {
                return nodeId;
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

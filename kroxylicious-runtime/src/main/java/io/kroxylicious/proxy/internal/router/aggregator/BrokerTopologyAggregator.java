/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import java.util.function.ToIntFunction;

import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;
import io.kroxylicious.proxy.service.HostPort;

/**
 * a router should implement this and register it as ResponseAggregator
 */
public class BrokerTopologyAggregator implements ApiMessageAggregator<ApiMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTopologyAggregator.class);

    private final EndpointGateway endpointGateway;

    private static final Integer NODE_ID_OFFSET = 10000;

    public BrokerTopologyAggregator(EndpointGateway endpointGateway) {
        this.endpointGateway = endpointGateway;
    }

    @Override
    public CompletionStage<AggregateResponse<ApiMessage>> aggregate(ResponseAggregationContext<ApiMessage> context) {
        return CompletableFuture.completedFuture(new AggregateResponse<ApiMessage>(context.firstHeader(), context.firstBody()));
    }

    private CompletionStage<AggregateResponse<MetadataResponseData>> handleMetadataResponse(ResponseAggregationContext<MetadataResponseData> context) {
        var nodeMap = new HashMap<Integer, Map<Integer, HostPort>>();
        var seenTopics = new HashSet<String>();
        context.responses().forEach((tc, response) -> {
            var tcNodeMap = new HashMap<Integer, HostPort>();
            nodeMap.put(tc.index(), tcNodeMap);
            for (MetadataResponseData.MetadataResponseBroker broker : response.brokers()) {
                tcNodeMap.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
                apply(broker, MetadataResponseData.MetadataResponseBroker::nodeId, MetadataResponseData.MetadataResponseBroker::host, MetadataResponseData.MetadataResponseBroker::port, MetadataResponseData.MetadataResponseBroker::setHost,
                        MetadataResponseData.MetadataResponseBroker::setPort, tc.index());
            }

            var updatedTopics = new MetadataResponseData.MetadataResponseTopicCollection();
            for (MetadataResponseData.MetadataResponseTopic topic : response.topics()) {
                if (seenTopics.contains(topic.name())) {
                    LOGGER.info("Duplicate topic {} found in metadata responses from multiple target clusters. Ignoring duplicate from {}", topic.name(), tc.name());
                    continue;
                }
                seenTopics.add(topic.name());
                for (MetadataResponseData.MetadataResponsePartition partition : topic.partitions()) {
                    partition.setReplicaNodes(
                            partition.replicaNodes().stream().map(i -> i + (tc.index() * NODE_ID_OFFSET)).toList()
                    );
                    partition.setIsrNodes(
                            partition.isrNodes().stream().map(i -> i + (tc.index() * NODE_ID_OFFSET)).toList()
                    );
                    partition.setOfflineReplicas(
                            partition.offlineReplicas().stream().map(i -> i + (tc.index() * NODE_ID_OFFSET)).toList()
                    );
                }
                updatedTopics.add(topic);
            }
            response.setTopics(updatedTopics);
        });

        return context.reconciler().reconcile(endpointGateway, nodeMap)
                .thenApply(v -> {
                    var firstEntry = context.responses().entrySet().iterator().next();
                    ResponseHeaderData header = context.headers().get(firstEntry.getKey());
                    MetadataResponseData response = firstEntry.getValue();
                    return new AggregateResponse<>(header, response);
                });
    }

    private <T> void apply(T broker, ToIntFunction<T> nodeIdGetter, Function<T, String> hostGetter, ToIntFunction<T> portGetter,
                           BiConsumer<T, String> hostSetter,
                           ObjIntConsumer<T> portSetter,
                           int tcIndex) {
        String incomingHost = hostGetter.apply(broker);
        int incomingPort = portGetter.applyAsInt(broker);

        int nodeId = nodeIdGetter.applyAsInt(broker) + (tcIndex * NODE_ID_OFFSET);
        var advertisedAddress = endpointGateway.getAdvertisedBrokerAddress(nodeId);

        LOGGER.trace("Rewriting broker address in response {}:{} -> {}", incomingHost, incomingPort, advertisedAddress);
        hostSetter.accept(broker, advertisedAddress.host());
        portSetter.accept(broker, advertisedAddress.port());
    }
}

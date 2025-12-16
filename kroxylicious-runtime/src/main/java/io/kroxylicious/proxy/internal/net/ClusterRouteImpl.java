/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.net;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.internal.session.BackendStateMachine;
import io.kroxylicious.proxy.net.ClusterRoute;

/**
 * Implementation of {@link ClusterRoute} that wraps a {@link BackendStateMachine}.
 */
public class ClusterRouteImpl implements ClusterRoute {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterRouteImpl.class);

    private final BackendStateMachine backendStateMachine;

    public ClusterRouteImpl(BackendStateMachine backendStateMachine) {
        this.backendStateMachine = Objects.requireNonNull(backendStateMachine);
    }

    @Override
    public String clusterId() {
        return backendStateMachine.clusterId();
    }

    @Override
    public String bootstrapServers() {
        return backendStateMachine.targetCluster().bootstrapServers();
    }

    @Override
    public Optional<Tls> tls() {
        return backendStateMachine.targetCluster().tls();
    }

    @Override
    public int nodeIdOffset() {
        return backendStateMachine.nodeIdOffset();
    }

    @Override
    public boolean isConnected() {
        return backendStateMachine.isConnected();
    }

    @Override
    public <M extends ApiMessage> CompletionStage<M> sendRequest(
                                                                 RequestHeaderData header,
                                                                 ApiMessage request) {

        Objects.requireNonNull(header, "header");
        Objects.requireNonNull(request, "request");

        if (!isConnected()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cluster not connected: " + clusterId()));
        }

        var apiKey = ApiKeys.forId(request.apiKey());
        header.setRequestApiKey(apiKey.id);
        header.setCorrelationId(-1); // Will be managed by the framework

        if (!apiKey.isVersionSupported(header.requestApiVersion())) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException(
                            "Cluster '%s': apiKey %s does not support version %d. Supported range: %d...%d"
                                    .formatted(clusterId(), apiKey, header.requestApiVersion(),
                                            apiKey.oldestVersion(), apiKey.latestVersion())));
        }

        var hasResponse = apiKey != ApiKeys.PRODUCE || ((ProduceRequestData) request).acks() != 0;

        LOGGER.debug("Cluster {}: Sending {} request (version {})",
                clusterId(), apiKey, header.requestApiVersion());

        // Delegate to backend's send request mechanism
        return backendStateMachine.sendRequest(header, request, hasResponse);
    }

    @Override
    public String toString() {
        return "ClusterRouteImpl{" +
                "clusterId='" + clusterId() + '\'' +
                ", bootstrapServers='" + bootstrapServers() + '\'' +
                ", nodeIdOffset=" + nodeIdOffset() +
                ", connected=" + isConnected() +
                '}';
    }
}

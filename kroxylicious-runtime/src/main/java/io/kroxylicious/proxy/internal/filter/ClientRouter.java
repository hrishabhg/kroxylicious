/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class ClientRouter implements ApiVersionsRequestFilter {

    private boolean routeSelected = false;

    private static final Map<String, TargetCluster> ClientRoutes = Map.of(
            "admin-1", new TargetCluster("localhost:9092", Optional.empty()),
            "clientX", new TargetCluster("localhost:9092", Optional.empty()),
            "clientY", new TargetCluster("localhost:50443", Optional.empty()),
            "default", new TargetCluster("localhost:9095", Optional.empty()) // error cluster
    );

    @Override
    public boolean shouldHandleApiVersionsRequest(short apiVersion) {
        return !routeSelected;
    }

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        String clientId = header.clientId();
        TargetCluster targetCluster = ClientRoutes.getOrDefault(clientId, ClientRoutes.get("default"));
        context.setTarget(targetCluster.bootstrapServers(), targetCluster.tls().orElse(null));
        routeSelected = true;
        return context.forwardRequest(header, request);
    }
}

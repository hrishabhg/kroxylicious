/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

/**
 * Aggregates ApiVersions responses from multiple target clusters.
 *
 * Logic:
 * - Returns error if any cluster reports one
 * - Otherwise, negotiates supported API versions across all clusters:
 *   - Min version = MAX of all cluster minimums (most restrictive)
 *   - Max version = MIN of all cluster maximums (most restrictive)
 * - If an API is not supported by a cluster, versions are set to 0
 *
 * This ensures the proxy only advertises API versions supported by ALL clusters.
 */
public class ApiVersionResponseAggregator implements ApiMessageAggregator<ApiVersionsResponseData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionResponseAggregator.class);

    @Override
    public CompletionStage<AggregateResponse<ApiVersionsResponseData>> aggregate(
            ResponseAggregationContext<ApiVersionsResponseData> context) {

        Objects.requireNonNull(context, "aggregation context cannot be null");

        var responses = context.responses();
        if (responses.isEmpty()) {
            String message = "No API versions responses to aggregate";
            LOGGER.error(message);
            return CompletableFuture.failedFuture(new IllegalArgumentException(message));
        }

        // Check if any response has an error
        boolean hasError = responses.values().stream()
                .peek(r -> LOGGER.trace("API versions response: error={}, apis={}",
                        r.errorCode(), r.apiKeys().size()))
                .anyMatch(response -> response.errorCode() != 0);

        if (hasError) {
            // Find the errored response and return it
            for (Map.Entry<TargetCluster, ApiVersionsResponseData> entry : responses.entrySet()) {
                TargetCluster targetCluster = entry.getKey();
                ApiVersionsResponseData response = entry.getValue();
                if (response.errorCode() != 0) {
                    LOGGER.warn("API versions error from cluster {}: code={}",
                            targetCluster, response.errorCode());

                    ApiVersionsResponseData errorResponse = new ApiVersionsResponseData();
                    errorResponse.setErrorCode(response.errorCode());

                    return CompletableFuture.completedFuture(
                            new AggregateResponse<>(
                                    context.headers().getOrDefault(targetCluster, new ResponseHeaderData()),
                                    errorResponse
                            )
                    );
                }
            }
        }

        // Negotiate API versions across all clusters
        Map<Short, ApiVersionsResponseData.ApiVersion> mergedVersions = proxyApiVersionsResponse();

        for (ApiVersionsResponseData response : responses.values()) {
            Map<Short, ApiVersionsResponseData.ApiVersion> versionMap = createVersionMap(response);

            for (Map.Entry<Short, ApiVersionsResponseData.ApiVersion> entry : mergedVersions.entrySet()) {
                Short apiKey = entry.getKey();
                ApiVersionsResponseData.ApiVersion mergedVersion = entry.getValue();
                ApiVersionsResponseData.ApiVersion responseVersion = versionMap.get(apiKey);

                if (responseVersion != null) {
                    // Negotiate: take the intersection
                    // Min version: most restrictive (maximum of minimums)
                    // Max version: most restrictive (minimum of maximums)
                    short newMinVersion = (short) Math.max(mergedVersion.minVersion(), responseVersion.minVersion());
                    short newMaxVersion = (short) Math.min(mergedVersion.maxVersion(), responseVersion.maxVersion());

                    LOGGER.trace("API {} negotiated: min=[{},{}]→{}, max=[{},{}]→{}",
                            apiKey,
                            mergedVersion.minVersion(), responseVersion.minVersion(), newMinVersion,
                            mergedVersion.maxVersion(), responseVersion.maxVersion(), newMaxVersion);

                    mergedVersion.setMinVersion(newMinVersion);
                    mergedVersion.setMaxVersion(newMaxVersion);
                }
                else {
                    // API not supported by this cluster: mark as unsupported
                    LOGGER.trace("API {} not supported by cluster, marking unsupported", apiKey);
                    mergedVersion.setMinVersion((short) 0);
                    mergedVersion.setMaxVersion((short) 0);
                }
            }
        }

        // Build response with negotiated versions
        ApiVersionsResponseData mergedResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersionCollection apiVersions = new ApiVersionsResponseData.ApiVersionCollection();
        apiVersions.addAll(mergedVersions.values());
        mergedResponse.setErrorCode((short) 0);
        mergedResponse.setApiKeys(apiVersions);

        LOGGER.debug("API versions aggregation complete: {} APIs negotiated across {} clusters",
                mergedVersions.size(), responses.size());

        return CompletableFuture.completedFuture(
                new AggregateResponse<>(context.firstHeader(), mergedResponse)
        );
    }

    /**
     * Creates a map of API version information indexed by API key for efficient lookup.
     */
    private Map<Short, ApiVersionsResponseData.ApiVersion> createVersionMap(ApiVersionsResponseData response) {
        return response.apiKeys().stream()
                .collect(Collectors.toMap(ApiVersionsResponseData.ApiVersion::apiKey, v -> v));
    }

    /**
     * Returns the set of APIs supported by the kafka-client library used by this proxy.
     * This is the starting point before negotiation with actual clusters.
     *
     * @return Map of API key to version information
     */
    private Map<Short, ApiVersionsResponseData.ApiVersion> proxyApiVersionsResponse() {
        Map<Short, ApiVersionsResponseData.ApiVersion> versionMap = new HashMap<>();

        for (ApiKeys apiKey : ApiKeys.values()) {
            versionMap.merge(
                    apiKey.id,
                    new ApiVersionsResponseData.ApiVersion()
                            .setApiKey(apiKey.id)
                            .setMinVersion(apiKey.messageType.lowestSupportedVersion())
                            .setMaxVersion(apiKey.latestVersion()),
                    (existing, incoming) -> existing  // Keep first occurrence
            );
        }

        LOGGER.debug("Proxy supports {} APIs", versionMap.size());
        return versionMap;
    }
}
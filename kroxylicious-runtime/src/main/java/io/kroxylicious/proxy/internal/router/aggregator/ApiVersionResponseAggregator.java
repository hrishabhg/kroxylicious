/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.AggregationContext;

/**
 * a router should implement this and register it as ResponseAggregator
 */
public class ApiVersionResponseAggregator implements ApiMessageAggregator<ApiVersionsResponseData> {

    @Override
    public ApiVersionsResponseData aggregateResponses(AggregationContext<ApiVersionsResponseData> context) {
        // check if any of the response has error
        var responses = context.responses();
        boolean hasError = responses.values().stream()
                .anyMatch(response -> response.errorCode() != 0);

        ApiVersionsResponseData mergedResponse = new ApiVersionsResponseData();
        if (hasError) {
            // if any response has error, set the error code to the first error code found
            for (ApiVersionsResponseData response : responses.values()) {
                if (response.errorCode() != 0) {
                    mergedResponse.setErrorCode(response.errorCode());
                    break;
                }
            }

            return mergedResponse;
        }

        Map<Short, ApiVersionsResponseData.ApiVersion> mergedVersions = proxyApiVersionsResponse();
        for (ApiVersionsResponseData response : responses.values()) {
            // max of min and min of max
            Map<Short, ApiVersionsResponseData.ApiVersion> versionMap = createVersionMap(response);
            for (Map.Entry<Short, ApiVersionsResponseData.ApiVersion> entry : mergedVersions.entrySet()) {
                Short apiKey = entry.getKey();
                ApiVersionsResponseData.ApiVersion mergedVersion = entry.getValue();
                ApiVersionsResponseData.ApiVersion responseVersion = versionMap.get(apiKey);
                if (responseVersion != null) {
                    short newMinVersion = (short) Math.max(mergedVersion.minVersion(), responseVersion.minVersion());
                    short newMaxVersion = (short) Math.min(mergedVersion.maxVersion(), responseVersion.maxVersion());
                    mergedVersion.setMinVersion(newMinVersion);
                    mergedVersion.setMaxVersion(newMaxVersion);
                }
                else {
                    // if the api key is not present in the response, set min and max to 0
                    mergedVersion.setMinVersion((short) 0);
                    mergedVersion.setMaxVersion((short) 0);
                }
            }
        }

        mergedResponse.setErrorCode((short) 0);



        return  mergedResponse;
    }

    private Map<Short, ApiVersionsResponseData.ApiVersion> createVersionMap(ApiVersionsResponseData response) {
        return response.apiKeys().stream()
                .collect(Collectors.toMap(ApiVersionsResponseData.ApiVersion::apiKey, v -> v));
    }

    /**
     * return ApiKeys supported by the kafka-client library used by the proxy
     * @return ApiVersionsResponseData
     */
    private Map<Short, ApiVersionsResponseData.ApiVersion> proxyApiVersionsResponse() {
        Map<Short, ApiVersionsResponseData.ApiVersion> versionMap = new HashMap<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            versionMap.merge(apiKey.id, new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiKey.id)
                    .setMinVersion(apiKey.messageType.lowestSupportedVersion())
                    .setMaxVersion(apiKey.latestVersion()), (existing, incoming) -> existing);
        }

        return versionMap;
    }
}

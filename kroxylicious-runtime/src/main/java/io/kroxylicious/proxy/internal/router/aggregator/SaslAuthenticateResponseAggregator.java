/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

/**
 * Aggregates SASL authentication responses from multiple target clusters.
 *
 * Logic:
 * - If any response contains an error, return that error response immediately
 * - If all responses succeed, return the first (successful) response
 *
 * All target clusters must reach the same authentication state.
 */
public class SaslAuthenticateResponseAggregator implements ApiMessageAggregator<SaslAuthenticateResponseData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslAuthenticateResponseAggregator.class);

    @Override
    public CompletionStage<AggregateResponse<SaslAuthenticateResponseData>> aggregate(
            ResponseAggregationContext<SaslAuthenticateResponseData> aggregationContext) {

        Objects.requireNonNull(aggregationContext, "aggregationContext cannot be null");

        Map<TargetCluster, SaslAuthenticateResponseData> responses = aggregationContext.responses();
        if (responses.isEmpty()) {
            String message = "No SASL authenticate responses to aggregate";
            LOGGER.error(message);
            return CompletableFuture.failedFuture(new IllegalArgumentException(message));
        }

        // if any response has an error, return it immediately
        for (Map.Entry<TargetCluster, SaslAuthenticateResponseData> entry : responses.entrySet()) {
            SaslAuthenticateResponseData response = entry.getValue();
            if (response.errorCode() != 0) {
                LOGGER.warn("SASL authenticate failed on cluster {}: error={}, message={}",
                        entry.getKey(), response.errorCode(), response.errorMessage());

                SaslAuthenticateResponseData errorResponse = new SaslAuthenticateResponseData();
                errorResponse.setErrorCode(response.errorCode());
                errorResponse.setErrorMessage(response.errorMessage());

                return CompletableFuture.completedFuture(
                        new AggregateResponse<>(aggregationContext.headers().get(entry.getKey()), errorResponse)
                );
            }
        }

        // all responses succeeded, return the first one
        LOGGER.debug("SASL authenticate succeeded on all {} clusters", responses.size());
        return CompletableFuture.completedFuture(
                new AggregateResponse<>(aggregationContext.firstHeader(), aggregationContext.firstBody())
        );
    }
}
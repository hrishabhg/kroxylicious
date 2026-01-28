/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

/**
 * Aggregates SASL handshake responses from multiple target clusters.
 *
 * Logic:
 * - Collects all supported mechanisms from all clusters (union)
 * - If any response contains an error, returns that error code along with mechanisms
 * - If all responses succeed, returns success with all mechanisms supported by any cluster
 */
public class SaslHandshakeResponseAggregator implements ApiMessageAggregator<SaslHandshakeResponseData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslHandshakeResponseAggregator.class);

    @Override
    public CompletionStage<AggregateResponse<SaslHandshakeResponseData>> aggregate(
            ResponseAggregationContext<SaslHandshakeResponseData> aggregationContext) {

        Objects.requireNonNull(aggregationContext, "aggregationContext cannot be null");

        Map<TargetCluster, SaslHandshakeResponseData> responses = aggregationContext.responses();
        if (responses.isEmpty()) {
            String message = "No SASL handshake responses to aggregate";
            LOGGER.error(message);
            return CompletableFuture.failedFuture(new IllegalArgumentException(message));
        }

        // Find if any response has an error
        OptionalInt anyError = responses.values().stream()
                .peek(r -> LOGGER.trace("SASL handshake response from cluster: error={}, mechanisms={}",
                        r.errorCode(), r.mechanisms().size()))
                .mapToInt(SaslHandshakeResponseData::errorCode)
                .filter(code -> code != 0)
                .findFirst();

        // Aggregate all supported mechanisms (union of all)
        List<String> supportedMechanisms = responses.values().stream()
                .flatMap(response -> response.mechanisms().stream())
                .distinct()
                .sorted()  // Deterministic ordering for testing and debugging
                .toList();

        if (anyError.isPresent()) {
            short errorCode = (short) anyError.getAsInt();
            LOGGER.warn("SASL handshake error detected: code={}, aggregated {} mechanisms",
                    errorCode, supportedMechanisms.size());

            return CompletableFuture.completedFuture(
                    new AggregateResponse<>(aggregationContext.firstHeader(),
                            new SaslHandshakeResponseData()
                                    .setErrorCode(errorCode)
                                    .setMechanisms(supportedMechanisms))
            );
        }

        LOGGER.debug("SASL handshake succeeded on all {} clusters, supporting {} mechanisms",
                responses.size(), supportedMechanisms.size());

        return CompletableFuture.completedFuture(
                new AggregateResponse<>(aggregationContext.firstHeader(),
                        new SaslHandshakeResponseData()
                                .setErrorCode((short) 0)
                                .setMechanisms(supportedMechanisms))
        );
    }
}
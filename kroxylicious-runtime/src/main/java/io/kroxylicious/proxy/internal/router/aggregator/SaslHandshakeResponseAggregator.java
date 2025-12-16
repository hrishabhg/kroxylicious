/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.kafka.common.message.SaslHandshakeResponseData;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.AggregationContext;

public class SaslHandshakeResponseAggregator implements ApiMessageAggregator<SaslHandshakeResponseData> {
    @Override
    public SaslHandshakeResponseData aggregateResponses(AggregationContext<SaslHandshakeResponseData> aggregationContext) {

        Map<TargetCluster, SaslHandshakeResponseData> responses = aggregationContext.responses();

        OptionalInt anyError = responses.values().stream()
                .mapToInt(SaslHandshakeResponseData::errorCode)
                .filter(code -> code != 0)
                .findFirst();

        List<String> supportedMechanisms = responses.values().stream()
                .flatMap(response -> response.mechanisms().stream())
                .distinct()
                .toList();

        if (anyError.isPresent()) {
            return new SaslHandshakeResponseData()
                    .setErrorCode((short) anyError.getAsInt())
                    .setMechanisms(supportedMechanisms);
        }

        return new SaslHandshakeResponseData()
                .setErrorCode((short) 0)
                .setMechanisms(supportedMechanisms);
    }
}

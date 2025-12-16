/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.Map;

import org.apache.kafka.common.message.SaslAuthenticateResponseData;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.AggregationContext;

public class SaslAuthenticateResponseAggregator implements ApiMessageAggregator<SaslAuthenticateResponseData> {

    @Override
    public SaslAuthenticateResponseData aggregateResponses(AggregationContext aggregationContext) {
        Map<TargetCluster, SaslAuthenticateResponseData> responses = aggregationContext.responses();
        // if no-error, return the first response (all should be identical); else return an error response
        for (SaslAuthenticateResponseData response : responses.values()) {
            if (response.errorCode() != 0) {
                SaslAuthenticateResponseData errorResponse = new SaslAuthenticateResponseData();
                errorResponse.setErrorCode(response.errorCode());
                errorResponse.setErrorMessage(response.errorMessage());
                return errorResponse;
            }
        }

        return responses.values().iterator().next();
    }

}

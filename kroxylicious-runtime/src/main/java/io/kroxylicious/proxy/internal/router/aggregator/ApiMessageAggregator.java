/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

public interface ApiMessageAggregator<B extends ApiMessage> {

    /**
     * @param aggregationContext correlation context for this aggregation
     * @return the aggregated response
     */
    CompletionStage<AggregateResponse<B>> aggregate(ResponseAggregationContext<B> aggregationContext);

}

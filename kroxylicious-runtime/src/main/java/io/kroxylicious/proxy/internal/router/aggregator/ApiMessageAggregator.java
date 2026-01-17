/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.internal.router.AggregationContext;

public interface ApiMessageAggregator<T extends ApiMessage> {

    /**
     *
     * @param responses response from each target cluster
     * @param aggregationContext correlation context for this aggregation
     * @return the aggregated response
     */
    T aggregateResponses(AggregationContext<T> aggregationContext);

}

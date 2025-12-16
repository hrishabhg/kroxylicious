/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.Map;

import org.apache.kafka.common.message.MetadataResponseData;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.router.AggregationContext;

/**
 * a router should implement this and register it as ResponseAggregator
 */
public class MetadataResponseAggregator implements ApiMessageAggregator<MetadataResponseData> {

    @Override
    public MetadataResponseData aggregateResponses(AggregationContext<MetadataResponseData> context) {
        return null;
    }
}

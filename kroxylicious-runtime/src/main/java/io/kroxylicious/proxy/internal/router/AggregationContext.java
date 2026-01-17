/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;

/**
 * Per request - single aggregation context to manage all responses being aggregated
 */
public class AggregationContext<B extends ApiMessage> {

    private final int expectedResponses;
    private final Map<TargetCluster, ResponseHeaderData> headers = new ConcurrentHashMap<>();
    private final Map<TargetCluster, B> responses = new ConcurrentHashMap<>();

    public AggregationContext(int expectedResponses) {
        this.expectedResponses = expectedResponses;
    }

    @SuppressWarnings("unchecked")
    public void addResponse(TargetCluster targetCluster, DecodedResponseFrame<?> frame) {
        this.addResponse(targetCluster, frame.header(), (B) frame.body());
    }

    public void addResponse(TargetCluster targetCluster, ResponseHeaderData header, B body) {
        headers.put(targetCluster, header);
        responses.put(targetCluster, body);
    }

    public int remainingResponses() {
        return expectedResponses - responses.size();
    }

    public boolean isComplete() {
        if (responses.size() > expectedResponses) {
            throw new IllegalStateException("Received more responses than expected");
        }
        return responses.size() == expectedResponses;
    }

    public Map<TargetCluster, B> responses() {
        return responses;
    }

    public Map<TargetCluster, ResponseHeaderData> headers() {
        return headers;
    }
}

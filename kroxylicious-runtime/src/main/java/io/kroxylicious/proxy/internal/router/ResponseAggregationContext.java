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
import io.kroxylicious.proxy.internal.net.EndpointReconciler;

/**
 * Per request - single aggregation context to manage all responses being aggregated
 */
public class ResponseAggregationContext<B extends ApiMessage> {

    private final int expectedResponses;
    private final Map<TargetCluster, ResponseHeaderData> headers = new ConcurrentHashMap<>();
    private final Map<TargetCluster, B> responses = new ConcurrentHashMap<>();
    private ResponseHeaderData firstHeader;
    private B firstBody;
    private EndpointReconciler reconciler;

    public ResponseAggregationContext(int expectedResponses, EndpointReconciler reconciler) {
        this.expectedResponses = expectedResponses;
        this.reconciler = reconciler;
    }

    @SuppressWarnings("unchecked")
    public void addMessage(TargetCluster targetCluster, DecodedResponseFrame<?> frame) {
        this.addMessage(targetCluster, frame.header(), (B) frame.body());
    }

    public void addMessage(TargetCluster targetCluster, ResponseHeaderData header, B body) {
        headers.put(targetCluster, header);
        responses.put(targetCluster, body);
        if (firstHeader == null) {
            firstHeader = header;
            firstBody = body;
        }
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

    public ResponseHeaderData firstHeader() {
        return firstHeader == null ? new ResponseHeaderData() : firstHeader;
    }

    public B firstBody() {
        return firstBody;
    }

    public EndpointReconciler reconciler() {
        return reconciler;
    }
}

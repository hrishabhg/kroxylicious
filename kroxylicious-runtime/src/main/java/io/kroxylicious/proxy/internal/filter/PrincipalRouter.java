/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.filter.ApiVersionsRequestFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.SaslAuthenticateRequestFilter;
import io.kroxylicious.proxy.filter.SaslHandshakeRequestFilter;

public class PrincipalRouter implements ApiVersionsRequestFilter, SaslHandshakeRequestFilter, SaslAuthenticateRequestFilter {

    private static final Map<String, TargetCluster> PrincipalRoutes = Map.of(
            "alice", new TargetCluster("localhost:9092", Optional.empty()),
            "bob", new TargetCluster("localhost:50443", Optional.empty()),
            "default", new TargetCluster("localhost:9095", Optional.empty())
    );

    @Override
    public CompletionStage<RequestFilterResult> onSaslAuthenticateRequest(short apiVersion, RequestHeaderData header, SaslAuthenticateRequestData request,
                                                                          FilterContext context) {
        // do not validate the password. extract the username and set the route
        // send the response back to the client
        // don't do for re-authentication
        if (!context.isTargetConnected()) {
            String username = getPrincipalFromAuthBytes(request.authBytes());
            TargetCluster targetCluster = PrincipalRoutes.getOrDefault(username, PrincipalRoutes.get("alice"));
            context.setTarget(targetCluster.bootstrapServers(), targetCluster.tls().orElse(null));
        }
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslAuthenticateResponseData().setAuthBytes("".getBytes(StandardCharsets.UTF_8)))
                .completed();
    }

    @Override
    public CompletionStage<RequestFilterResult> onSaslHandshakeRequest(short apiVersion, RequestHeaderData header, SaslHandshakeRequestData request,
                                                                       FilterContext context) {
        // return plain sasl mechanism for simplicity
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(new SaslHandshakeResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setMechanisms(List.of("PLAIN")))
                .completed();
    }

    private String getPrincipalFromAuthBytes(byte[] authBytes) {
        String authString = new String(authBytes, StandardCharsets.UTF_8);
        String[] tokens = authString.split("\0");
        if (tokens.length >= 2) {
            return tokens[1]; // authcid
        }
        return "anonymous";
    }

    @Override
    public CompletionStage<RequestFilterResult> onApiVersionsRequest(short apiVersion, RequestHeaderData header, ApiVersionsRequestData request, FilterContext context) {
        ApiVersionsResponseData response = new ApiVersionsResponseData();
        response.setErrorCode(Errors.NONE.code());

        // Advertise all API keys supported by the Kafka client library.
        // This ensures the client sees support for SASL_HANDSHAKE and SASL_AUTHENTICATE.
        for (ApiKeys key : ApiKeys.values()) {
            response.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(key.id)
                    .setMinVersion(key.oldestVersion())
                    .setMaxVersion(key.latestVersion()));
        }

        return context.requestFilterResultBuilder()
                .shortCircuitResponse(response)
                .completed();
    }
}

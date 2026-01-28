/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

//import java.util.List;
//import java.util.Objects;
//
//import org.apache.kafka.common.protocol.ApiKeys;
//
//import io.kroxylicious.proxy.service.UpstreamEndpoint;
//
//import edu.umd.cs.findbugs.annotations.Nullable;

///**
// * A bootstrap binding.
// *
// * @param endpointGateway the endpoint gateway
// */
//public record BootstrapEndpointBinding(EndpointGateway endpointGateway) implements EndpointBinding {
//
//    public BootstrapEndpointBinding {
//        Objects.requireNonNull(endpointGateway, "endpointGateway cannot be null");
//    }
//
//    @Override
//    public List<UpstreamEndpoint> upstreamServiceEndpoints(ApiKeys apiKey) {
//        return allUpstreamServiceEndpoints();
//    }
//
//    @Override
//    public List<UpstreamEndpoint> allUpstreamServiceEndpoints() {
//        return endpointGateway.targetClusters().stream().map(t -> new UpstreamEndpoint(t.bootstrapServer().host(), t.bootstrapServer().port(), t)).toList();
//    }
//
//    @Nullable
//    @Override
//    public Integer nodeId() {
//        return null;
//    }
//}
public interface BootstrapEndpointBinding extends EndpointBinding { }

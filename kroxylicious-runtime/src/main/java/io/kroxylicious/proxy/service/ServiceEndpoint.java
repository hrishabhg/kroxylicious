/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.TargetCluster;

// aka HostPort with Tls
public record ServiceEndpoint(String host, int port, TargetCluster targetCluster) {
    public HostPort getHostPort() {
        return new HostPort(host, port);
    }
}

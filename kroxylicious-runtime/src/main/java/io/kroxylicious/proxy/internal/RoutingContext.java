/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.config.TargetCluster;

public class RoutingContext {

    private TargetCluster targetCluster;
    private boolean connected = false;

    public RoutingContext(TargetCluster targetCluster) {
        this.targetCluster = targetCluster;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public void setTargetCluster(TargetCluster targetCluster) {
        this.targetCluster = targetCluster;
    }

    public void connected() {
        connected = true;
    }

    public boolean isConnected() {
        return connected;
    }

}

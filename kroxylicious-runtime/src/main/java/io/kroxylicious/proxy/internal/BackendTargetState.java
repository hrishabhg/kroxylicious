/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Connection-scoped state for backend target selection.
 * <p>
 * This class holds the target backend Kafka cluster information that a filter
 * can set during request processing. Once set, the target cannot be changed
 * for the lifetime of the connection.
 * </p>
 * <p>
 * Thread-safety: This class is thread-safe. The target can be set exactly once,
 * and subsequent reads will see the set value.
 * </p>
 */
public class BackendTargetState {

    private volatile String bootstrapServers;
    private volatile @Nullable Tls tlsConfig;
    private volatile boolean selected = false;

    /**
     * Sets the target backend Kafka cluster for this connection.
     * <p>
     * This method can only be called once per connection. Subsequent calls
     * will throw {@link IllegalStateException}.
     * </p>
     *
     * @param bootstrapServers Bootstrap servers address (e.g., "kafka-1:9092,kafka-2:9092")
     * @param tlsConfig TLS configuration, or null if plaintext connection to backend
     * @throws IllegalStateException if target has already been selected
     * @throws NullPointerException if bootstrapServers is null
     */
    public synchronized void setTarget(String bootstrapServers, @Nullable Tls tlsConfig) {
        if (selected) {
            throw new IllegalStateException("Backend target already selected for this connection");
        }
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers cannot be null");
        this.tlsConfig = tlsConfig;
        this.selected = true;
    }

    /**
     * @return true if a filter has selected the backend target for this connection
     */
    public boolean isSelected() {
        return selected;
    }

    /**
     * @return The bootstrap servers if selected, empty otherwise
     */
    public Optional<String> bootstrapServers() {
        return selected ? Optional.of(bootstrapServers) : Optional.empty();
    }

    /**
     * @return The TLS config if selected and TLS is configured, empty otherwise.
     *         Note: empty can mean either "not selected yet" or "selected with no TLS".
     *         Use {@link #isSelected()} to distinguish.
     */
    public Optional<Tls> tlsConfig() {
        return selected ? Optional.ofNullable(tlsConfig) : Optional.empty();
    }

    @Override
    public String toString() {
        if (!selected) {
            return "BackendTargetState{not selected}";
        }
        return "BackendTargetState{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", tlsConfig=" + (tlsConfig != null ? "configured" : "none") +
                '}';
    }
}
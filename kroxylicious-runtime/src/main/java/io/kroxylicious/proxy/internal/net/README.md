# Multi-Cluster Routing Architecture

This package provides the infrastructure for multi-cluster routing in Kroxylicious proxy.

## Overview

The multi-cluster architecture allows filters to:
- Connect to multiple Kafka clusters simultaneously
- Route requests to specific clusters
- Fan-out requests to all clusters and merge responses
- Apply nodeId offsets to prevent ID collisions across clusters

## Package Structure

```
io.kroxylicious.proxy.internal.net/
├── RoutingContextImpl.java    # Runtime implementation of RoutingContext
└── ClusterRouteImpl.java      # Runtime implementation of ClusterRoute
```

## Core Components

### 1. RoutingContext (API)

The `RoutingContext` interface (in `io.kroxylicious.proxy.net`) provides filters with:

```java
public interface RoutingContext {
    // Cluster discovery
    boolean isMultiCluster();
    Set<String> clusterIds();
    Optional<ClusterRoute> cluster(String clusterId);
    ClusterRoute primaryCluster();
    
    // Request routing
    <M> CompletionStage<M> sendRequest(String clusterId, RequestHeaderData header, ApiMessage request);
    <M> CompletionStage<Map<String, M>> fanOutRequest(Set<String> clusterIds, ...);
    <M> CompletionStage<Map<String, M>> fanOutRequestToAll(...);
    
    // NodeId utilities for metadata merging
    int applyNodeIdOffset(int nodeId, String clusterId);
    int removeNodeIdOffset(int adjustedNodeId, String clusterId);
    Optional<String> clusterForNodeId(int adjustedNodeId);
}
```

### 2. ClusterRoute (API)

Represents a single cluster connection:

```java
public interface ClusterRoute {
    String clusterId();
    String bootstrapServers();
    int nodeIdOffset();
    boolean isConnected();
    <M> CompletionStage<M> sendRequest(RequestHeaderData header, ApiMessage request);
}
```

### 3. NetFilter (API)

Determines which cluster(s) to connect to:

```java
public interface NetFilter {
    void selectServer(NetFilterContext context, RoutingContext routingContext);
    
    interface NetFilterContext {
        // Single cluster (backward compatible)
        void initiateConnect(HostPort target, Optional<SslContext> ssl, List<Object> filters);
        
        // Multi-cluster
        void initiateMultiClusterConnect(
            Map<String, TargetCluster> clusters,
            Map<String, Optional<SslContext>> sslContexts,
            List<Object> filters);
    }
}
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT                                          │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      KafkaProxyFrontendHandler                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                   ClientSessionStateMachine                          │    │
│  │  States: Startup → ClientActive → ApiVersions → Routing → Closed    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                   ClusterConnectionManager                           │    │
│  │  Manages multiple BackendStateMachine instances                      │    │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                  │    │
│  │  │ Backend "A"  │ │ Backend "B"  │ │ Backend "C"  │                  │    │
│  │  │ offset=0     │ │ offset=1000  │ │ offset=2000  │                  │    │
│  │  └──────────────┘ └──────────────┘ └──────────────┘                  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     RoutingContextImpl                               │    │
│  │  Wraps ClusterConnectionManager for filter access                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                    │                    │                    │
                    ▼                    ▼                    ▼
            ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
            │  Cluster A   │     │  Cluster B   │     │  Cluster C   │
            │  (Primary)   │     │              │     │              │
            └──────────────┘     └──────────────┘     └──────────────┘
```

## Request Flow

### Path 1: Normal Request (forwardRequest)

```
Filter.forwardRequest(header, request)
    │
    ▼
ctx.fireChannelRead(frame)
    │
    ▼
[Remaining Filters in Pipeline]
    │
    ▼
FrontendHandler.channelRead()
    │
    ▼
ClientSessionStateMachine.onClientRequest()
    │
    ▼
ClusterConnectionManager.forwardToServer()
    │
    ▼
BackendStateMachine.forwardToServer()  ─────► [Primary Cluster]
```

### Path 2: Async Request (routingContext.sendRequest)

```
Filter.routingContext().sendRequest("clusterB", header, request)
    │
    ▼
RoutingContextImpl.sendRequest()
    │
    ▼
ClusterRouteImpl.sendRequest()
    │
    ▼
BackendStateMachine.sendRequest()
    │
    ├── Assign correlationId = -1, -2, ... (negative)
    ├── Store in pendingAsyncRequests map
    └── Write DecodedRequestFrame to channel
                    │
                    ▼
              [Cluster B]
                    │
                    ▼
         Response with correlationId=-1
                    │
                    ▼
BackendHandlerAdapter.channelRead()
    │
    ├── correlationId < 0? YES
    └── Complete promise, DO NOT forward to frontend
```

### Path 3: Fan-out Request

```
Filter.routingContext().fanOutRequestToAll(header, metadataRequest)
    │
    ├── Clone request for each cluster
    ├── Send to Cluster A → CompletableFuture<MetadataResponse>
    ├── Send to Cluster B → CompletableFuture<MetadataResponse>
    └── Send to Cluster C → CompletableFuture<MetadataResponse>
                    │
                    ▼
    CompletableFuture.allOf().thenApply(...)
                    │
                    ▼
    Map<String, MetadataResponse> responses
                    │
                    ▼
    Filter merges responses, applying nodeId offsets
```

## NodeId Offset Handling

Each cluster is assigned a nodeId offset to prevent collisions:

| Cluster | Offset | NodeId Range |
|---------|--------|--------------|
| A (primary) | 0 | 0-999 |
| B | 1000 | 1000-1999 |
| C | 2000 | 2000-2999 |

### Applying Offsets (Response Merging)

```java
// In metadata filter
MetadataResponseData merged = new MetadataResponseData();
for (var entry : responses.entrySet()) {
    String clusterId = entry.getKey();
    MetadataResponseData clusterResponse = entry.getValue();
    
    for (var broker : clusterResponse.brokers()) {
        // Apply offset to nodeId
        int adjustedNodeId = routingContext.applyNodeIdOffset(broker.nodeId(), clusterId);
        merged.brokers().add(new MetadataResponseData.MetadataResponseBroker()
            .setNodeId(adjustedNodeId)
            .setHost(broker.host())
            .setPort(broker.port()));
    }
}
```

### Removing Offsets (Request Routing)

```java
// In produce filter - route to correct cluster based on partition leader
int leaderId = partitionInfo.leaderId();  // e.g., 1005

Optional<String> clusterId = routingContext.clusterForNodeId(leaderId);
// clusterId = Optional.of("B") because 1005 is in range 1000-1999

int originalNodeId = routingContext.removeNodeIdOffset(leaderId, clusterId.get());
// originalNodeId = 5
```

## Correlation ID Management

To distinguish async (filter-initiated) requests from client requests:

- **Client requests**: correlationId >= 0 (assigned by client)
- **Async requests**: correlationId < 0 (assigned by BackendStateMachine)

This ensures:
1. No correlation ID collisions
2. Async responses are intercepted before reaching the frontend
3. Client request/response flow is unaffected

## Connection Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONNECTION STATES                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ClientSessionStateMachine:                                      │
│    Startup → ClientActive → ApiVersions → Routing → Closed      │
│                                                                  │
│  BackendConnectionState (per cluster):                          │
│    Created → Connecting → Connected → Closed                    │
│                      ↘      ↓                                    │
│                       Failed                                     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Backpressure

Backpressure flows in two directions:

### Client → Backend (Slow Backend)

```
Backend unwritable
    ↓
BackendStateMachine.onChannelUnwritable()
    ↓
ClusterConnectionManager.onBackendUnwritable()
    ↓
ClientSessionStateMachine.onBackendUnwritable()
    ↓
clientReadsBlocked = true
    ↓
frontendHandler.applyBackpressure()
    ↓
clientChannel.config().setAutoRead(false)
```

### Backend → Client (Slow Client)

```
Client unwritable
    ↓
KafkaProxyFrontendHandler.channelWritabilityChanged()
    ↓
ClientSessionStateMachine.onClientUnwritable()
    ↓
ClusterConnectionManager.applyBackpressureToAll()
    ↓
Each BackendStateMachine.applyBackpressure()
    ↓
backendChannel.config().setAutoRead(false)
```

## Usage Examples

### Example 1: Simple Single-Cluster Filter

```java
public class MyFilter implements RequestFilter {
    @Override
    public CompletionStage<RequestFilterResult> onRequest(..., FilterContext context) {
        // Access primary cluster
        ClusterRoute primary = context.routingContext().primaryCluster();
        
        // Forward normally
        return context.forwardRequest(header, request);
    }
}
```

### Example 2: Multi-Cluster Metadata Aggregation

```java
public class MetadataAggregationFilter implements RequestFilter {
    @Override
    public CompletionStage<RequestFilterResult> onMetadataRequest(
            short apiVersion, RequestHeaderData header, 
            MetadataRequestData request, FilterContext context) {
        
        RoutingContext routing = context.routingContext();
        
        if (!routing.isMultiCluster()) {
            return context.forwardRequest(header, request);
        }
        
        // Fan-out to all clusters
        return routing.<MetadataResponseData>fanOutRequestToAll(header, request)
            .thenApply(responses -> {
                MetadataResponseData merged = mergeMetadata(responses, routing);
                return context.requestFilterResultBuilder()
                    .shortCircuitResponse(merged)
                    .build();
            });
    }
    
    private MetadataResponseData mergeMetadata(
            Map<String, MetadataResponseData> responses, 
            RoutingContext routing) {
        // Merge logic with nodeId offset handling
        // ...
    }
}
```

### Example 3: Coordinator Routing

```java
public class CoordinatorRoutingFilter implements RequestFilter {
    private final Map<String, String> groupToCluster = new ConcurrentHashMap<>();
    
    @Override
    public CompletionStage<RequestFilterResult> onFindCoordinatorRequest(...) {
        String groupId = request.key();
        String targetCluster = groupToCluster.get(groupId);
        
        if (targetCluster == null) {
            // First request - discover and cache
            return discoverCoordinator(groupId, context);
        }
        
        // Route to known cluster
        return context.routingContext()
            .<FindCoordinatorResponseData>sendRequest(targetCluster, header, request)
            .thenApply(response -> adjustResponse(response, targetCluster, context));
    }
}
```

## Thread Safety

All components are designed to run on a single Netty event loop:
- No synchronization needed for single-connection state
- `ConcurrentHashMap` used for cross-connection caches
- CompletionStage callbacks execute on the connection's event loop

## Configuration

Multi-cluster is enabled via the NetFilter's `selectServer` implementation:

```java
// In NetFilter implementation
@Override
public void selectServer(NetFilterContext context, RoutingContext routingContext) {
    Map<String, TargetCluster> clusters = Map.of(
        "east", new TargetCluster("kafka-east:9092", Optional.empty()),
        "west", new TargetCluster("kafka-west:9092", Optional.empty())
    );
    
    context.initiateMultiClusterConnect(clusters, Map.of(), filters);
}
```

## Error Handling

- If primary cluster fails during connect: session closes
- If secondary cluster fails: continues with remaining clusters
- If all clusters fail: session closes with error
- Async request timeout: promise completed exceptionally

## Future Enhancements

1. **Request timeout** - Configurable timeout for async requests
2. **Circuit breaker** - Automatic failover between clusters
3. **Load balancing** - Distribute requests across clusters
4. **Cluster health monitoring** - Proactive connection management

# Apache Kafka Connect Distributed Mode: Architecture and Assignment Allocation

## Overview

Apache Kafka Connect is a scalable, fault-tolerant framework designed for streaming data between Apache Kafka and other systems. In **distributed mode**, Connect leverages Kafka's consumer group protocol to coordinate multiple worker nodes, automatically balancing connectors and tasks across the cluster for high availability and scalability.

## Key Architecture Components

### 1. WorkerCoordinator - Group Membership Management

The `WorkerCoordinator` class is the cornerstone of distributed coordination, extending Kafka's `AbstractCoordinator` to manage worker group membership:

```java
public class WorkerCoordinator extends AbstractCoordinator
```

**Key Responsibilities:**
- **Group Membership**: Workers join a consumer group using `group.id` configuration
- **Leader Election**: One worker is elected as the leader responsible for assignment decisions  
- **Heartbeat Management**: Maintains session with Kafka brokers via periodic heartbeats
- **Configuration Synchronization**: Ensures all workers have consistent view of connector configs
- **Rebalance Coordination**: Orchestrates the redistribution of work when cluster membership changes

### 2. DistributedHerder - Distributed Orchestration

The `DistributedHerder` serves as the main orchestrator for the distributed Connect cluster:

```java
public class DistributedHerder extends AbstractHerder implements Runnable
```

**Core Functions:**
- **Connector Lifecycle Management**: Creates, starts, stops, and deletes connectors
- **Task Distribution**: Coordinates task assignment across workers
- **Configuration Management**: Handles connector configuration updates
- **REST API Integration**: Processes REST requests for cluster management
- **Rebalance Handling**: Manages worker coordination during rebalances

### 3. Assignment Algorithms

Connect employs sophisticated assignment algorithms to distribute work optimally:

#### EagerAssignor (Round-Robin Strategy)
```java
public class EagerAssignor implements ConnectorAssignor
```
- **Simple Distribution**: Uses round-robin allocation for connectors and tasks
- **Immediate Rebalancing**: Stops all tasks during rebalancing (stop-the-world approach)
- **Legacy Protocol**: Used with `connect.protocol=eager`

#### IncrementalCooperativeAssignor (Advanced Strategy) 
```java
public class IncrementalCooperativeAssignor implements ConnectorAssignor
```
- **Incremental Rebalancing**: Only affects connectors/tasks that need to move
- **Cooperative Protocol**: Minimizes disruption during rebalancing
- **Load Balancing**: Implements sophisticated load balancing with revocation delays
- **Default Protocol**: Used with `connect.protocol=compatible` (default)

## Distributed Mode Architecture Flow

### 1. Worker Startup and Group Formation

```
1. Worker Process Starts
   ├── Load Configuration (bootstrap.servers, group.id, etc.)
   ├── Initialize WorkerCoordinator
   ├── Initialize DistributedHerder  
   └── Join Consumer Group

2. Group Coordination
   ├── Send JoinGroup Request to Kafka
   ├── Leader Election (if first worker or leader failed)
   ├── Metadata Exchange (current assignments, capabilities)
   └── Assignment Computation (by leader)
```

### 2. Connector and Task Assignment Process

#### Leader Worker Responsibilities:
```java
// From IncrementalCooperativeAssignor
public Map<String, ByteBuffer> performAssignment(String leaderId, 
                                                String protocol,
                                                List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata)
```

**Assignment Algorithm Steps:**
1. **Gather Current State**: Collect current assignments from all workers
2. **Read Configuration Topic**: Get latest connector configurations from `connect-configs`
3. **Compute New Assignment**: Distribute connectors and tasks optimally
4. **Handle Revocations**: Determine which resources need to be moved
5. **Apply Balancing**: Ensure even distribution across workers
6. **Send Assignments**: Distribute new assignments to all workers

#### Assignment Data Structure:
```java
public class ExtendedAssignment {
    private final short error;
    private final String leader;
    private final String leaderUrl;
    private final long offset;
    private final List<String> connectors;
    private final List<ConnectorTaskId> tasks;
    private final List<String> revokedConnectors;    // New in cooperative protocol
    private final List<ConnectorTaskId> revokedTasks; // New in cooperative protocol
    private final int delay;                          // Rebalance delay
}
```

### 3. Work Distribution Strategies

#### Basic Distribution Pattern:
```
Cluster: 3 Workers (W1, W2, W3)
Connectors: A, B, C (each with 2 tasks)

Round-Robin Assignment:
W1: [Connector-A, Task-A-0, Task-B-1, Task-C-0]
W2: [Connector-B, Task-A-1, Task-C-1]  
W3: [Connector-C, Task-B-0]
```

#### Load Balancing Considerations:
- **Connector Distribution**: Each connector runs on exactly one worker
- **Task Distribution**: Tasks can be distributed across any available workers
- **Resource Balancing**: Algorithm tries to balance total workload across workers
- **Fault Tolerance**: Failed workers trigger reassignment of their resources

## Rebalancing Mechanisms

### Traditional Eager Rebalancing
```
Rebalance Trigger (e.g., worker joins)
    ↓
1. Stop ALL connectors/tasks on ALL workers
    ↓
2. Release all resources
    ↓  
3. Rejoin group and wait for new assignment
    ↓
4. Start assigned connectors/tasks
```

### Incremental Cooperative Rebalancing (KIP-415)
```
Rebalance Trigger
    ↓
1. Keep running current assignments
    ↓
2. Leader computes minimal changes needed
    ↓
3. Send revocation requests for specific resources
    ↓
4. Workers stop only revoked resources
    ↓
5. Rejoin and receive new assignments
    ↓
6. Start only newly assigned resources
```

**Benefits of Cooperative Rebalancing:**
- **Reduced Downtime**: Only affected connectors/tasks are disrupted
- **Faster Recovery**: Minimal resource movement during rebalancing
- **Better Availability**: Unaffected work continues processing
- **Graceful Scaling**: Smooth addition/removal of workers

## Configuration Topics and State Management

### Internal Topics:
1. **`connect-configs`**: Stores connector and task configurations
2. **`connect-offsets`**: Stores source connector offsets  
3. **`connect-status`**: Stores connector and task status information

### State Synchronization:
```java
// WorkerCoordinator ensures consistent view
private void updateSnapshot(ConnectProtocol.Assignment assignment) {
    this.assignmentSnapshot = assignment;
    recomputeTaskConfigs();
}
```

## Fault Tolerance and High Availability

### Worker Failure Handling:
1. **Detection**: Missing heartbeats trigger failure detection
2. **Grace Period**: `scheduled.rebalance.max.delay.ms` allows temporary failures
3. **Reassignment**: Failed worker's connectors/tasks redistributed
4. **Recovery**: Returning workers can reclaim previous assignments

### Configuration Persistence:
- All configurations stored in Kafka topics (replicated and durable)
- Workers maintain consistent view through configuration topic consumption
- Offset management ensures exactly-once processing for source connectors

## Monitoring and Observability

### Key Metrics:
- **Rebalance Frequency**: `completed-rebalances-total`
- **Rebalance Latency**: `rebalance-avg-time-ms`
- **Assignment State**: `connector-count`, `task-count`
- **Worker Status**: `rebalancing` flag

### REST API Endpoints:
```
GET /connectors                    # List all connectors
GET /connectors/{name}/status      # Get connector and task status  
GET /connectors/{name}/tasks       # List connector tasks
POST /connectors/{name}/restart    # Restart connector/tasks
```

## Best Practices for Production

### Cluster Sizing:
- **3+ Workers**: Minimum for high availability
- **Resource Planning**: Consider connector/task resource requirements
- **Network Topology**: Co-locate with Kafka brokers when possible

### Configuration Recommendations:
```properties
# Worker Group Configuration  
group.id=connect-cluster
config.storage.topic=connect-configs
offset.storage.topic=connect-offsets
status.storage.topic=connect-status

# Rebalancing Configuration
connect.protocol=compatible              # Enable cooperative rebalancing
scheduled.rebalance.max.delay.ms=300000 # 5min grace period
session.timeout.ms=30000                # Worker session timeout
heartbeat.interval.ms=3000              # Heartbeat frequency

# Topic Configuration (high replication for durability)
config.storage.replication.factor=3
offset.storage.replication.factor=3  
status.storage.replication.factor=3
```

## Conclusion

Apache Kafka Connect's distributed mode provides a robust, scalable architecture for streaming data integration. Through sophisticated coordinator protocols, intelligent assignment algorithms, and incremental cooperative rebalancing, Connect ensures high availability and efficient resource utilization across worker clusters. The framework's use of Kafka's own coordination mechanisms provides strong consistency guarantees while enabling seamless scaling and fault tolerance for production data streaming workloads.

IIAXSISII: For IncrementalCooperativeAssignor how does it get set to the cluster ?

GitHub Copilot: Let me examine how the `IncrementalCooperativeAssignor` gets configured and set for a Kafka Connect cluster. 

Searched codebase for "IncrementalCooperativeAssignor configuration setup assignment protocol connect.protocol", 11 results

Now let me look for how the protocol compatibility is configured and how the assignor is selected: 

Searched text for `connect.protocol|ConnectProtocolCompatibility|COMPATIBLE|EAGER`, 20 results

Now let me look at the ConnectProtocolCompatibility enum and how it's configured: 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/ConnectProtocolCompatibility.java)

Now let me look at how the protocol configuration is handled in DistributedConfig: 

Searched text for `CONNECT_PROTOCOL_CONFIG|connect\.protocol`, 18 results

Let me read the DistributedConfig file to see the complete configuration setup: 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/DistributedConfig.java#465-465), lines 465 to 505

Let me look at the default values: 

Searched text for `CONNECT_PROTOCOL_DEFAULT`, 2 results

Now let me look at how the WorkerCoordinator selects which assignor to use: 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/WorkerCoordinator.java#225-225), lines 225 to 240

Based on my analysis of the code, here's how the `IncrementalCooperativeAssignor` gets set for a Kafka Connect cluster:

## How IncrementalCooperativeAssignor Gets Set to the Cluster

### 1. Configuration-Based Selection

The assignor is selected based on the `connect.protocol` configuration property in the worker configuration:

**Configuration Property:**
```properties
connect.protocol=<value>
```

**Valid Values:**
- `eager` → Uses `EagerAssignor` (legacy, stop-the-world rebalancing)
- `compatible` → Uses `IncrementalCooperativeAssignor` (protocol v1)
- `sessioned` → Uses `IncrementalCooperativeAssignor` (protocol v2 with session keys)

**Default Value:**
```java
public static final String CONNECT_PROTOCOL_DEFAULT = ConnectProtocolCompatibility.SESSIONED.toString();
// Default is "sessioned" which uses IncrementalCooperativeAssignor
```

### 2. Assignor Instantiation in WorkerCoordinator

Both assignors are created during `WorkerCoordinator` initialization:

```java
public WorkerCoordinator(GroupRebalanceConfig config,
                         LogContext logContext,
                         ConsumerNetworkClient client,
                         Metrics metrics,
                         String metricGrpPrefix,
                         Time time,
                         String restUrl,
                         ConfigBackingStore configStorage,
                         WorkerRebalanceListener listener,
                         ConnectProtocolCompatibility protocolCompatibility,
                         int maxDelay) {
    // ...
    this.incrementalAssignor = new IncrementalCooperativeAssignor(logContext, time, maxDelay);
    this.eagerAssignor = new EagerAssignor(logContext);
    this.currentConnectProtocol = protocolCompatibility;
    // ...
}
```

### 3. Runtime Selection Logic

During rebalancing, the actual assignor is chosen based on the negotiated protocol:

```java
@Override
protected Map<String, ByteBuffer> onLeaderElected(String leaderId, 
                                                  String protocol,
                                                  List<JoinGroupResponseMember> allMemberMetadata,
                                                  boolean skipAssignment) {
    ConnectProtocolCompatibility protocolCompatibility = ConnectProtocolCompatibility.fromProtocol(protocol);
    return protocolCompatibility == EAGER
           ? eagerAssignor.performAssignment(leaderId, protocolCompatibility, allMemberMetadata, this)
           : incrementalAssignor.performAssignment(leaderId, protocolCompatibility, allMemberMetadata, this);
}
```

### 4. Protocol Negotiation Process

The protocol selection happens through Kafka's group coordination protocol:

1. **Worker Startup**: Each worker announces which protocols it supports
2. **Protocol Negotiation**: Kafka broker coordinator selects the common protocol supported by all workers
3. **Assignor Selection**: The leader worker uses the appropriate assignor based on the negotiated protocol

**Protocol Support Matrix:**
```java
switch (currentConnectProtocol) {
    case EAGER -> ConnectProtocol.metadataRequest(workerState);                    // V0
    case COMPATIBLE -> IncrementalCooperativeConnectProtocol.metadataRequest(workerState, false); // V1
    case SESSIONED -> IncrementalCooperativeConnectProtocol.metadataRequest(workerState, true);   // V2
}
```

### 5. Configuration in Practice

**To use IncrementalCooperativeAssignor, set in worker properties:**
```properties
# For protocol v1 (incremental cooperative rebalancing)
connect.protocol=compatible

# For protocol v2 (incremental cooperative + session keys) - DEFAULT
connect.protocol=sessioned

# To disable and use legacy eager rebalancing
connect.protocol=eager
```

### 6. Mixed Cluster Support

The system supports mixed clusters during rolling upgrades:

- **Compatible Protocol**: Workers with `connect.protocol=compatible` can work with both eager and incremental protocols
- **Downgrade Path**: If any worker uses `eager`, the entire cluster falls back to eager rebalancing
- **Upgrade Path**: All workers must support the same protocol level for incremental features

### Summary

The `IncrementalCooperativeAssignor` is automatically selected when:
1. Workers are configured with `connect.protocol=compatible` or `connect.protocol=sessioned` (default)
2. All workers in the cluster support incremental cooperative protocol
3. The Kafka group coordinator negotiates a protocol version ≥ 1

The selection is **automatic and transparent** to users - they simply need to configure the `connect.protocol` property, and the framework handles the rest through the protocol negotiation process.

IIAXSISII: For the IncrementalCooperativeAssignor strategy, I want to enhance it to include capacity aware allocation. Currently when a node is taken out of the spot cluster, when it rebalances it assigns in an even way thinking all tasks are evenly consuming resources, but in reality some tasks are heavy vs other based on throughput. I want to introduce a config tasks.weight for connector config which is an integer that can range from 1 to 5 that can be set where when set to 1 it will exhibit current default behaviour and it is the default value. But when its set to 5 it will treat it as the highest throughput tasks where it needs to dedicate a full 1 vCPU to the task. Similar to assigned tasks to workers, it will need to inform each other nodes about the available cpu cores in each node where it will not allocate too many 5s in the same node. It will look for workers that has evenly distributed numbers when its set to higher number. 

Review the code and suggest if this is possible first before making the changes ?

GitHub Copilot: I'll analyze the `IncrementalCooperativeAssignor` code to understand its current architecture and assess the feasibility of adding capacity-aware allocation based on task weights and CPU cores. 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java#1-1), lines 1 to 101

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java#130-130), lines 130 to 230

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java#390-390), lines 390 to 470

Searched text for `assignConnectors` (`**/IncrementalCooperativeAssignor.java`), 4 results

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java#770-770), lines 770 to 810

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/IncrementalCooperativeAssignor.java#800-800), lines 800 to 840

Now let me look at the WorkerLoad class to understand how it tracks worker assignments: 

Searched codebase for "WorkerLoad class builder assign connector task", 9 results

Now let me look for how connector configuration is handled and if we can access task weights: 

Searched text for `ClusterConfigState` (`**/IncrementalCooperativeAssignor.java`), 6 results

Searched codebase for "ClusterConfigState connector configuration", 6 results

Summarized conversation historyGreat! Now I have a much clearer picture of the `ClusterConfigState` class. Let me examine more details about how configurations are accessed and explore system topology detection. 

Searched text for `cpu|CPU|vcpu|vCPU|core` (`**/connect/runtime/**/*.java`), 20 results

The grep search didn't find any CPU-related logic in the Connect runtime. Let me search more broadly for system resource management. 

Searched codebase for "system resources memory capacity worker available", 23 results

Excellent! I found the `WorkerInfo` class that includes CPU information. Let me read more about this class to understand the system resource detection capabilities. 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/WorkerInfo.java)

Perfect! This is exactly what I need. The `WorkerInfo` class already has system resource detection capabilities including CPU count via `OS.getAvailableProcessors()`. Now let me examine how this information is used in the Worker and if there are any existing capacity-aware mechanisms. 

Searched text for `WorkerInfo` (`**/connect/runtime/**/*.java`), 6 results

Now let me read the `AbstractConnectCli` to see how `WorkerInfo` is currently used: 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/cli/AbstractConnectCli.java#110-110), lines 110 to 130

Good! I can see that `WorkerInfo` is used during worker initialization to log system information. Now I have enough information to provide a comprehensive feasibility assessment for the capacity-aware allocation enhancement. Let me check one more thing - the current configuration structure to understand where the task weight configuration would fit. 

Searched codebase for "task config configuration properties", 24 results

Perfect! Now I have a comprehensive understanding of the Kafka Connect architecture and can provide a detailed feasibility assessment for the capacity-aware allocation enhancement.

## Feasibility Assessment: Capacity-Aware Task Allocation Enhancement

Based on my comprehensive analysis of the Apache Kafka Connect distributed architecture, I can now provide a detailed feasibility assessment for your proposed capacity-aware allocation enhancement.

### **Current Architecture Overview**

The current assignment mechanism in `IncrementalCooperativeAssignor` uses:
1. **Simple Round-Robin Distribution**: Tasks and connectors are distributed evenly across workers using `WorkerLoad.connectorComparator()` and `WorkerLoad.taskComparator()`
2. **Count-Based Load Balancing**: Load balancing is based purely on the number of connectors and tasks assigned to each worker
3. **No Resource Awareness**: No consideration of CPU capacity, memory, or workload characteristics

### **Enhancement Feasibility: HIGHLY FEASIBLE ✅**

Your proposed enhancement is **technically feasible** and would integrate well with the existing architecture. Here's the detailed assessment:

## **Integration Points Analysis**

### **1. Configuration Integration**
- **Location**: Add `tasks.weight` to `ConnectorConfig` alongside existing `TASKS_MAX_CONFIG`
- **Implementation**: Extend the existing ConfigDef in ConnectorConfig.java:
  ```java
  public static final String TASKS_WEIGHT_CONFIG = "tasks.weight";
  private static final String TASKS_WEIGHT_DOC = 
      "Task weight factor (1-5) indicating CPU intensity. 1=default behavior, 5=dedicated vCPU";
  public static final int TASKS_WEIGHT_DEFAULT = 1;
  ```
- **Access Pattern**: Configuration flows through `ClusterConfigState.connectorConfig()` → assignment algorithms

### **2. System Resource Detection**
- **Current Capability**: `WorkerInfo` class already detects CPU count via `OS.getAvailableProcessors()`
- **Enhancement**: Extend `WorkerLoad` class to include CPU capacity tracking:
  ```java
  public static class WorkerLoad {
      private final String worker;
      private final List<String> connectors;
      private final List<ConnectorTaskId> tasks;
      private final int availableCpuCores;      // NEW
      private final int currentCpuLoad;         // NEW
      // ... existing methods
  }
  ```

### **3. Assignment Algorithm Enhancement**
- **Current Method**: `performTaskAssignment()` in IncrementalCooperativeAssignor.java
- **Enhancement Strategy**: Replace simple count-based comparators with weighted capacity-aware comparators
- **Key Changes**:
  ```java
  // Enhanced comparator considering CPU capacity and task weights
  public static Comparator<WorkerLoad> capacityAwareTaskComparator() {
      return Comparator.comparing(WorkerLoad::effectiveCpuLoad)
                       .thenComparing(WorkerLoad::taskCount);
  }
  
  private double effectiveCpuLoad() {
      return (double) currentCpuLoad / availableCpuCores;
  }
  ```

## **Implementation Architecture**

### **Phase 1: Data Structure Enhancement**
1. **Extend WorkerLoad Class**:
   - Add CPU capacity fields
   - Implement weighted load calculation methods
   - Create capacity-aware comparators

2. **Configuration Access**:
   - Modify assignment methods to access connector configurations
   - Extract task weight from connector config during assignment

### **Phase 2: Assignment Logic Modification**
1. **Enhanced assignTasks() Method**:
   ```java
   private void assignTasks(Map<String, Collection<ConnectorTaskId>> tasksByConnector,
                           List<WorkerLoad> workerLoads,
                           ClusterConfigState configState) {
       for (Map.Entry<String, Collection<ConnectorTaskId>> entry : tasksByConnector.entrySet()) {
           String connectorName = entry.getKey();
           int taskWeight = getTaskWeight(connectorName, configState);
           
           for (ConnectorTaskId taskId : entry.getValue()) {
               WorkerLoad worker = findBestWorkerForTask(workerLoads, taskWeight);
               worker.assignTask(taskId, taskWeight);
           }
       }
   }
   ```

2. **Capacity-Aware Worker Selection**:
   ```java
   private WorkerLoad findBestWorkerForTask(List<WorkerLoad> workers, int taskWeight) {
       return workers.stream()
                    .filter(w -> w.canAccommodateTask(taskWeight))
                    .min(WorkerLoad.capacityAwareTaskComparator())
                    .orElse(workers.get(0)); // Fallback to least loaded
   }
   ```

### **Phase 3: Backward Compatibility**
- **Default Behavior**: When `tasks.weight` is not set or equals 1, use existing round-robin logic
- **Gradual Migration**: Workers can handle mixed assignments (weighted and unweighted tasks)
- **Configuration Validation**: Validate weight values (1-5) during connector creation

## **Technical Benefits**

### **1. Resource Optimization**
- **CPU-Aware Distribution**: High-weight tasks get dedicated CPU resources
- **Load Balancing**: Prevents CPU-intensive tasks from being co-located
- **Scalability**: Better resource utilization in heterogeneous clusters

### **2. Operational Benefits**
- **Predictable Performance**: Weight 5 tasks get consistent performance
- **Fine-Grained Control**: Operators can tune task distribution based on workload characteristics
- **Monitoring Integration**: Enhanced metrics for capacity-aware monitoring

## **Implementation Complexity**

### **Low Complexity Components**
- ✅ Configuration integration (`ConnectorConfig` extension)
- ✅ System resource detection (already available in `WorkerInfo`)
- ✅ Data structure enhancement (`WorkerLoad` modifications)

### **Medium Complexity Components**
- ⚠️ Assignment algorithm modification (requires careful testing)
- ⚠️ Rebalancing logic updates (ensure incremental cooperative behavior)
- ⚠️ Integration testing with existing assignment protocols

### **Risk Mitigation**
- **Comprehensive Testing**: Unit tests for new comparators and assignment logic
- **Integration Testing**: Test with various cluster configurations and workloads
- **Performance Testing**: Ensure no regression in assignment performance
- **Fallback Mechanisms**: Graceful degradation to current behavior when needed

## **Recommended Implementation Sequence**

1. **Phase 1** (2-3 weeks): Extend data structures and configuration
2. **Phase 2** (3-4 weeks): Implement capacity-aware assignment algorithms
3. **Phase 3** (2-3 weeks): Testing, documentation, and refinement
4. **Phase 4** (1-2 weeks): Integration testing and performance validation

## **Conclusion**

Your capacity-aware allocation enhancement is **highly feasible** and represents a valuable improvement to Kafka Connect's distributed architecture. The existing codebase provides excellent foundation points for integration, and the enhancement follows established patterns in the Connect framework.

**Key Success Factors**:
- Leverage existing `WorkerInfo` CPU detection capabilities
- Build upon proven `WorkerLoad` and assignment patterns
- Maintain backward compatibility through configuration defaults
- Ensure thorough testing of rebalancing scenarios

The enhancement would provide significant value for production deployments where CPU-intensive connectors need predictable resource allocation while maintaining the flexibility and reliability of the current distributed coordination mechanism.

IIAXSISII: Make those changes to the codebase to incorporate the Capacity-Aware Task Allocation Enhancement based on exact steps you proposed in previous answer.

GitHub Copilot: I'll implement the capacity-aware task allocation enhancement following the phased approach I outlined. Let me start by examining the current code structure and implementing the changes systematically. 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#130-130), lines 130 to 180

Now let me check where the configuration is defined in the ConfigDef: 

Read [](file:///Users/sgautam/git/tl/kafka/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ConnectorConfig.java#700-700), lines 700 to 760
<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Global Task Balance for Kafka Connect

## Overview

The Global Task Balance feature provides enhanced task distribution for Kafka Connect distributed mode, prioritizing global balance of tasks across all worker nodes over locality and continuity.

## What is Global Task Balance?

**Default Behavior (IncrementalCooperativeAssignor):**
- Kafka Connect tends to co-locate connector tasks on the same workers for efficiency
- When workers fail, their tasks are reassigned but may still cluster together
- This can lead to uneven load distribution across the cluster

**Global Task Balance (GlobalBalanceAssignor):**
- **Prioritizes even distribution** of tasks across all available workers
- **Tasks for the same connector** are deliberately spread across different workers
- **Guarantees perfect load balancing** with maximum difference of 1 task between any two workers
- **Uses two assignment strategies:**
  - **Round-robin assignment** for full rebalances (when all tasks need reassignment)
  - **Load-aware assignment** for incremental changes (assigns to least loaded workers)

## Key Features

### Perfect Load Balancing
The GlobalBalanceAssignor guarantees mathematically optimal task distribution:
- For N tasks and W workers, each worker gets either floor(N/W) or ceil(N/W) tasks
- Maximum difference between any two workers is always 1 task
- Uses deterministic assignment for consistent results

### Two Assignment Modes

#### 1. Full Rebalance (Round-Robin)
Used when all tasks need to be reassigned (e.g., configuration changes):
- Clears all current assignments
- Distributes tasks using round-robin algorithm
- Achieves perfect balance: tasks 0,3,6... → worker1, tasks 1,4,7... → worker2, etc.

#### 2. Incremental Assignment (Load-Aware)  
Used for new/unassigned tasks (e.g., scaling up connectors):
- Preserves existing assignments
- Assigns each new task to the least loaded worker
- Maintains balance while minimizing disruption

### Deterministic Assignment
- Workers are sorted alphabetically for consistent assignment
- Tasks are sorted by connector name, then task ID
- Same assignment results across multiple rebalances

## Configuration

### Enabling Global Task Balance

Add the following property to your Connect worker configuration:

```properties
# Enable global task balance (default: false)
global.task.balance.enabled=true
```

### Example Configuration File

Create a worker configuration file (e.g., `connect-distributed-global-balance.properties`):

```properties
# Basic Connect configuration
bootstrap.servers=localhost:9092
group.id=connect-cluster
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
offset.storage.topic=connect-offsets
config.storage.topic=connect-configs
status.storage.topic=connect-status

# Enable global task balance
global.task.balance.enabled=true

# Other distributed Connect settings
offset.storage.replication.factor=3
config.storage.replication.factor=3
status.storage.replication.factor=3
```

## Behavior Examples

### Basic Example: Perfect Balance with 100 Tasks and 3 Workers

This demonstrates the core requirement of maintaining a maximum difference of 1 task between workers:

**Setup:**
- Workers: worker1, worker2, worker3
- Total tasks: 100 (from various connectors)

**Global Balance Distribution:**
```
worker1: 34 tasks (tasks 0, 3, 6, 9, 12, ...)
worker2: 33 tasks (tasks 1, 4, 7, 10, 13, ...)
worker3: 33 tasks (tasks 2, 5, 8, 11, 14, ...)
```

**Result:** Maximum difference = 1 (34 - 33 = 1) ✓

### Scenario: 5 Workers, 2 Connectors

**Setup:**
- Workers: W1, W2, W3, W4, W5
- Connector C1: 8 tasks
- Connector C2: 5 tasks
- Total: 13 tasks

**Default Behavior (global.task.balance.enabled=false):**
```
W1: C1-T0, C1-T1, C1-T2, C1-T3 (4 C1 tasks)
W2: C1-T4, C1-T5, C1-T6, C1-T7 (4 C1 tasks)
W3: C2-T0, C2-T1, C2-T2 (3 C2 tasks)
W4: C2-T3, C2-T4 (2 C2 tasks)
W5: (empty)
Task distribution: [4, 4, 3, 2, 0] - difference = 4
```

**Global Balance Behavior (global.task.balance.enabled=true):**
```
W1: C1-T0, C1-T5, C2-T0 (3 total tasks)
W2: C1-T1, C1-T6, C2-T1 (3 total tasks)
W3: C1-T2, C1-T7, C2-T2 (3 total tasks)
W4: C1-T3, C2-T3 (2 total tasks)
W5: C1-T4, C2-T4 (2 total tasks)
Task distribution: [3, 3, 3, 2, 2] - difference = 1 ✓
```

### Example: Incremental Assignment

**Initial State (3 workers, existing tasks):**
```
W1: C1-T0, C1-T3 (2 tasks)
W2: C1-T1, C1-T4 (2 tasks)  
W3: C1-T2 (1 task)
```

**New Tasks Added: C2-T0, C2-T1, C2-T2**

**Load-Aware Assignment Process:**
1. C2-T0 → W3 (least loaded: 1 task) → W3 now has 2 tasks
2. C2-T1 → W1 (tied at 2 tasks, but alphabetically first) → W1 now has 3 tasks  
3. C2-T2 → W2 (tied at 2 tasks) → W2 now has 3 tasks

**Final State:**
```
W1: C1-T0, C1-T3, C2-T1 (3 tasks)
W2: C1-T1, C1-T4, C2-T2 (3 tasks)
W3: C1-T2, C2-T0 (2 tasks)
Task distribution: [3, 3, 2] - difference = 1 ✓
```

### Example: Full Rebalance Scenario

When a full rebalance is triggered (e.g., configuration change), all assignments are cleared and redistributed:

**Before (unbalanced state):**
```
W1: C1-T0, C1-T1, C1-T2, C1-T3, C1-T4 (5 tasks)
W2: C2-T0, C2-T1 (2 tasks)
W3: (empty)
```

**After Full Rebalance (round-robin redistribution):**
```
W1: C1-T0, C1-T3, C2-T1 (3 tasks) - indices 0, 3, 6
W2: C1-T1, C1-T4 (2 tasks) - indices 1, 4  
W3: C1-T2, C2-T0 (2 tasks) - indices 2, 5
Task distribution: [3, 2, 2] - difference = 1 ✓
```

### Benefits of Global Balance

1. **Perfect Load Distribution**: Mathematical guarantee of maximum 1 task difference between workers
2. **Better Resource Utilization**: All workers participate equally in processing
3. **Improved Fault Tolerance**: Connector tasks spread across multiple workers  
4. **Reduced Hotspots**: No single worker becomes overloaded
5. **Deterministic Assignment**: Consistent results across rebalances
6. **Minimal Disruption**: Load-aware assignment preserves existing assignments when possible

## Algorithm Details

### Round-Robin Assignment (Full Rebalance)

Used when `isFullRebalance = true` (all tasks are already assigned but need redistribution):

```java
// Simplified algorithm
tasks.sort(byConnectorThenTaskId);
workers.sort(byWorkerName);

int workerIndex = 0;
for (ConnectorTaskId task : tasks) {
    WorkerLoad targetWorker = workers.get(workerIndex % workers.size());
    targetWorker.assign(task);
    workerIndex++;
}
```

**Mathematical Properties:**
- For N tasks and W workers: worker_i gets floor(N/W) + (1 if i < N%W else 0) tasks
- Maximum difference between workers = 1 (when N%W > 0) or 0 (when N%W = 0)
- Example: 100 tasks, 3 workers → [34, 33, 33] (difference = 1)

### Load-Aware Assignment (Incremental)

Used for new/unassigned tasks while preserving existing assignments:

```java
// Simplified algorithm  
tasks.sort(byConnectorThenTaskId);

for (ConnectorTaskId task : tasks) {
    workers.sort(byTaskCountThenWorkerName);  // Least loaded first
    WorkerLoad leastLoaded = workers.get(0);
    leastLoaded.assign(task);
}
```

**Properties:**
- Always assigns to the worker with fewest current tasks
- Maintains perfect balance as new tasks are added
- Preserves existing task assignments (no revocations)
- Deterministic tie-breaking using worker names

## When to Use Global Task Balance

### ✅ **Recommended When:**
- You have many workers and want to utilize all of them equally
- Load balancing is more important than co-location efficiency  
- Connectors have many tasks that can be distributed
- You want to avoid worker hotspots and achieve perfect balance
- Fault tolerance across workers is a priority
- You need predictable, deterministic task distribution

### ❌ **Not Recommended When:**
- You have very few workers (2-3 workers)
- Co-location efficiency is critical for performance
- Connectors have very few tasks (1-2 tasks per connector)
- Network latency between workers is high
- You need maximum single-connector throughput

## 🧪 **Comprehensive Testing**

### **Test Coverage:**
The implementation includes comprehensive test suites:

- `GlobalBalanceAssignorTest.java` - Core algorithm testing
- `GlobalBalanceAssignorScenarioTest.java` - Real-world scenario validation
- `GlobalBalanceAssignorAutoscalingTest.java` - Cloud autoscaling scenarios
- `DistributedConfigTest.java` - Configuration property testing

### **Autoscaling Test Scenarios:**
1. **testMultipleWorkerFailuresWithPreemptibleInstances()** - Universal preemptible handling
2. **testWorkerScaleDown()** - Universal scale-down scenarios
3. **testWorkerScaleUp()** - Universal scale-up scenarios
4. **Load balancing verification** - Mathematical distribution validation
5. **Minimal movement validation** - Stability preservation testing

## Implementation Details

### Core Components

1. **DistributedConfig**: Added `global.task.balance.enabled` configuration property
2. **GlobalBalanceAssignor**: Enhanced assignor implementing round-robin and load-aware strategies
3. **WorkerCoordinator**: Integration point that selects the appropriate assignor based on configuration

### Assignment Strategy Selection

```java
// In WorkerCoordinator.performAssignment()
boolean globalBalanceEnabled = distributedConfig.globalTaskBalanceEnabled();

if (protocolCompatibility == EAGER) {
    return eagerAssignor.performAssignment(...);
} else if (globalBalanceEnabled) {
    return globalBalanceAssignor.performAssignment(...);
} else {
    return incrementalAssignor.performAssignment(...);
}
```

### Assignment Mode Logic

The GlobalBalanceAssignor uses two different strategies:

1. **Full Rebalance Detection:**
   ```java
   Set<ConnectorTaskId> currentlyAssigned = getCurrentlyAssignedTasks(workers);
   Collection<ConnectorTaskId> unassignedTasks = getUnassignedTasks(tasks, currentlyAssigned);
   boolean isFullRebalance = unassignedTasks.isEmpty() && !tasks.isEmpty();
   ```

2. **Strategy Selection:**
   - If `isFullRebalance`: Clear all assignments and use round-robin
   - If `!unassignedTasks.isEmpty()`: Use load-aware assignment for unassigned tasks
   - Otherwise: No assignment needed

### Core Algorithm Methods

#### `assignTasksRoundRobin()`
- Sorts workers alphabetically for deterministic assignment  
- Sorts tasks by connector name, then task ID
- Uses simple modulo operation: `workerIndex % workers.size()`
- Guarantees perfect balance with maximum difference of 1

#### `assignTasksLoadAware()`
- Preserves existing task assignments
- For each new task, sorts workers by current load (ascending)
- Assigns to the least loaded worker
- Uses worker name as tie-breaker for determinism

#### `assignConnectorsWithGlobalBalance()`  
- Distributes connector assignments evenly across workers
- Sorts workers by current connector load before each assignment
- Assigns each connector to the least loaded worker

## Backward Compatibility

- **Default Value**: `global.task.balance.enabled=false` (maintains existing behavior)
- **Existing Clusters**: No impact unless explicitly enabled
- **Configuration**: Optional property - clusters work normally without it
- **Protocol**: Uses existing incremental cooperative rebalancing protocol

## Monitoring and Verification

### Check Task Distribution

Use the Connect REST API to verify task distribution:

```bash
# Get connector status
curl http://localhost:8083/connectors/my-connector/status

# Check which worker is running each task
curl http://localhost:8083/connectors/my-connector/tasks
```

### Expected Output with Global Balance

You should see tasks for the same connector distributed across different worker URLs rather than clustered on the same workers.

## Testing

### Unit Tests

The implementation includes comprehensive tests:

- `GlobalBalanceAssignorTest`: Core algorithm testing
- `GlobalBalanceAssignorScenarioTest`: Real-world scenario validation
- `DistributedConfigTest`: Configuration property testing

### Integration Testing

1. Start multiple Connect workers with global balance enabled
2. Create connectors with multiple tasks
3. Verify task distribution using REST API
4. Simulate worker failures and verify rebalancing

## Troubleshooting

### Common Issues

**Tasks still co-located after enabling:**
- Verify `global.task.balance.enabled=true` in worker config
- Restart all workers to pick up configuration changes
- Check that rebalancing has occurred

**Performance impact:**
- Monitor connector throughput after enabling
- Consider network latency between workers
- Evaluate if co-location efficiency loss is acceptable

**Uneven distribution:**
- Check number of tasks vs number of workers
- Verify all workers are healthy and participating
- Review connector task configuration

### Expected Output with Global Balance

You should see tasks for the same connector distributed across different worker URLs rather than clustered on the same workers.

## Troubleshooting

### Common Issues

**Tasks still co-located after enabling:**
- Verify `global.task.balance.enabled=true` in worker config
- Restart all workers to pick up configuration changes
- Check that rebalancing has occurred

**Performance impact:**
- Monitor connector throughput after enabling
- Consider network latency between workers
- Evaluate if co-location efficiency loss is acceptable

**Uneven distribution:**
- Check number of tasks vs number of workers
- Verify all workers are healthy and participating
- Review connector task configuration

### Logs

Enable debug logging to see assignment decisions:

```properties
log4j.logger.org.apache.kafka.connect.runtime.distributed.GlobalBalanceAssignor=DEBUG
log4j.logger.org.apache.kafka.connect.runtime.distributed.WorkerCoordinator=DEBUG
```

### Disabling Global Balance

1. **Remove/Disable Configuration**: Set `global.task.balance.enabled=false`
2. **Rolling Restart**: Restart workers
3. **Natural Rebalance**: Next rebalance will return to default behavior

## Version Information

- **Built On**: Kafka Connect 3.7.2  
- **Kafka Version Compatibility**: 3.7.2+
- **API Compatibility**: No breaking changes
- **Configuration**: Single property: `global.task.balance.enabled`

## Related Documentation

- [Kafka Connect Distributed Mode](https://kafka.apache.org/documentation/#connect_running)
- [Connect Configuration](https://kafka.apache.org/documentation/#connectconfigs)
- [Connect REST API](https://kafka.apache.org/documentation/#connect_rest)
- [Incremental Cooperative Rebalancing](https://kafka.apache.org/documentation/#connect_rebalancing)
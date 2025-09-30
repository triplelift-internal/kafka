# Global Task Balance for Kafka Connect

## Overview

This feature adds global task balancing capabilities to Kafka Connect distributed mode, allowing tasks from the same connector to be spread evenly across all available worker nodes instead of being co-located on the same workers.

## What is Global Task Balance?

**Default Behavior (Before):**
- Kafka Connect tends to co-locate connector tasks on the same workers for efficiency
- When workers fail, their tasks are reassigned but may still cluster together
- This can lead to uneven load distribution across the cluster

**Global Task Balance (New):**
- Prioritizes even distribution of tasks across all available workers
- Tasks for the same connector are deliberately spread across different workers
- Provides better load balancing at the cost of some co-location efficiency

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

### Scenario: 5 Workers, 2 Connectors

**Setup:**
- Workers: W1, W2, W3, W4, W5
- Connector C1: 8 tasks
- Connector C2: 5 tasks

**Default Behavior (global.task.balance.enabled=false):**
```
W1: C1-T0, C1-T1, C1-T2, C1-T3 (4 C1 tasks)
W2: C1-T4, C1-T5, C1-T6, C1-T7 (4 C1 tasks)
W3: C2-T0, C2-T1, C2-T2 (3 C2 tasks)
W4: C2-T3, C2-T4 (2 C2 tasks)
W5: (empty)
```

**Global Balance Behavior (global.task.balance.enabled=true):**
```
W1: C1-T0, C1-T5, C2-T0 (mixed tasks - 3 total)
W2: C1-T1, C1-T6, C2-T1 (mixed tasks - 3 total)
W3: C1-T2, C1-T7, C2-T2 (mixed tasks - 3 total)
W4: C1-T3, C2-T3 (mixed tasks - 2 total)
W5: C1-T4, C2-T4 (mixed tasks - 2 total)
```

### Benefits of Global Balance

1. **Even Load Distribution**: Tasks spread across all workers (3-3-3-2-2 vs 4-4-3-2-0)
2. **Better Fault Tolerance**: Connector failures affect multiple workers, not just a few
3. **Resource Utilization**: All workers participate in processing every connector
4. **Reduced Hotspots**: No single worker becomes overloaded with tasks from one connector

## When to Use Global Task Balance

### ✅ **Recommended When:**
- You have many workers and want to utilize all of them
- Load balancing is more important than co-location efficiency
- Connectors have many tasks that can be distributed
- You want to avoid worker hotspots
- Fault tolerance across workers is a priority

### ❌ **Not Recommended When:**
- You have few workers (2-3 workers)
- Co-location efficiency is critical for performance
- Connectors have very few tasks
- Network latency between workers is high
- You need maximum connector task throughput

## Implementation Details

### Core Components

1. **DistributedConfig**: Added `global.task.balance.enabled` configuration property
2. **GlobalBalanceAssignor**: New assignor implementing round-robin task distribution
3. **WorkerCoordinator**: Integration point that selects the appropriate assignor
4. **WorkerLoad**: Enhanced with connector-specific task counting methods

### Algorithm

The global balance algorithm works as follows:

1. **Group by Connector**: Collect all tasks for each connector
2. **Round-Robin Distribution**: For each connector, distribute its tasks in round-robin fashion across all workers
3. **Balance Optimization**: Ensure the most even distribution possible (max difference of 1 task per worker)

```java
// Simplified algorithm
for each connector:
    for each task in connector:
        assign task to workers[taskIndex % numWorkers]
        taskIndex++
```

## Backward Compatibility

- **Default Value**: `global.task.balance.enabled=false` (maintains existing behavior)
- **Existing Clusters**: No impact unless explicitly enabled
- **Configuration**: Optional property - clusters work normally without it
- **Protocol**: Uses existing rebalancing protocol - no breaking changes

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

### Logs

Enable debug logging to see assignment decisions:

```properties
log4j.logger.org.apache.kafka.connect.runtime.distributed.GlobalBalanceAssignor=DEBUG
log4j.logger.org.apache.kafka.connect.runtime.distributed.WorkerCoordinator=DEBUG
```

## Migration Guide

### Enabling on Existing Cluster

1. **Add Configuration**: Add `global.task.balance.enabled=true` to all worker configs
2. **Rolling Restart**: Restart workers one by one
3. **Trigger Rebalance**: The next rebalance will use global balance
4. **Verify Distribution**: Check task assignment via REST API

### Disabling Global Balance

1. **Remove/Disable Configuration**: Set `global.task.balance.enabled=false`
2. **Rolling Restart**: Restart workers
3. **Natural Rebalance**: Next rebalance will return to default behavior

## Performance Considerations

### Pros
- Better resource utilization across all workers
- Improved fault tolerance
- More predictable load distribution

### Cons
- Potential increase in network traffic between workers
- Loss of co-location efficiency benefits
- Slightly more complex failure scenarios

## Version Information

- **Introduced**: Kafka 4.2.0-SNAPSHOT
- **Kafka Version Compatibility**: 4.2.0+
- **API Compatibility**: No breaking changes

## Related Documentation

- [Kafka Connect Distributed Mode](https://kafka.apache.org/documentation/#connect_running)
- [Connect Configuration](https://kafka.apache.org/documentation/#connectconfigs)
- [Connect REST API](https://kafka.apache.org/documentation/#connect_rest)

---

*For additional details, see the implementation files:*
- `GlobalBalanceAssignor.java` - Core algorithm implementation
- `DistributedConfig.java` - Configuration management  
- `WorkerCoordinator.java` - Integration and orchestration
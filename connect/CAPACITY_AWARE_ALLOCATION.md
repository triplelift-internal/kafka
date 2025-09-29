# Capacity-Aware Task Allocation Enhancement

## Overview

This enhancement introduces capacity-aware task allocation to Apache Kafka Connect's distributed mode, allowing operators to specify task CPU requirements and ensuring optimal resource distribution across worker nodes.

## Key Features

### 1. Task Weight Configuration
- **Configuration**: `tasks.weight` (integer, range 1-100)
- **Default**: 1 (maintains backward compatibility)
- **Behavior**: 
  - Weight 1: Lightweight task (100 tasks can share one CPU core)
  - Weight 50: Moderate task (2 tasks per CPU core)
  - Weight 100: Heavy task (dedicated CPU core per task)

### 2. CPU Capacity Detection
- Automatically detects available CPU cores per worker
- Tracks current CPU load based on assigned task weights
- Prevents CPU oversubscription

### 3. Intelligent Assignment Algorithm
- **Capacity-aware**: Considers CPU utilization when assigning tasks
- **Load balancing**: Distributes tasks based on effective CPU load rather than task count
- **Fallback**: Gracefully handles workers at capacity by using least loaded worker

## Configuration Example

```properties
# Traditional connector (uses round-robin)
connector.class=org.apache.kafka.connect.file.FileSourceConnector
tasks.max=3

# CPU-intensive connector (requires dedicated resources)
connector.class=com.example.HighThroughputConnector
tasks.max=2
tasks.weight=100

# Medium-intensity connector
connector.class=com.example.MediumThroughputConnector  
tasks.max=4
tasks.weight=50

# Lightweight connector (many concurrent tasks)
connector.class=com.example.LightweightConnector
tasks.max=20
tasks.weight=5
```

## Implementation Details

### Enhanced WorkerLoad Class
```java
public class WorkerLoad {
    private final int availableCpuCores;     // Detected CPU cores
    private int currentCpuLoad;              // Sum of task weights
    
    // Total capacity = cores * 100 (weight units per core)
    public int totalCpuCapacity() { return availableCpuCores * 100; }
    public boolean canAccommodateTask(int weight);
    public double effectiveCpuLoad();
    public static Comparator<WorkerLoad> capacityAwareTaskComparator();
}
```

### Assignment Logic Flow
1. **Detection**: Check if any connectors have `tasks.weight > 1`
2. **Strategy Selection**:
   - Weighted tasks present → Use capacity-aware allocation
   - All tasks weight=1 → Use traditional round-robin (highly concurrent)
3. **Worker Selection**: Sort workers by CPU utilization ratio
4. **Assignment**: Place tasks on workers with sufficient capacity (each core = 100 weight units)

## Benefits

### Resource Optimization
- **Predictable Performance**: High-weight tasks get dedicated CPU resources
- **Prevent Contention**: Avoids co-locating CPU-intensive tasks
- **Better Utilization**: Distributes workload based on actual resource needs

### Operational Benefits
- **Tunable Performance**: Fine-grained control over task resource allocation
- **Monitoring Integration**: CPU load metrics for capacity planning
- **Backward Compatible**: Existing deployments continue working unchanged

## Migration Path

### Phase 1: Deploy Enhancement
- Deploy updated Connect runtime with capacity-aware features
- All existing connectors continue using traditional assignment (weight=1)

### Phase 2: Identify CPU-Intensive Connectors
- Monitor connector performance and resource usage
- Identify connectors that would benefit from dedicated resources

### Phase 3: Configure Task Weights
- Add `tasks.weight` configuration to appropriate connectors
- Weight 1-10: Very lightweight tasks (high concurrency)
- Weight 20-40: Moderate resource usage
- Weight 60-80: CPU-intensive operations
- Weight 100: Maximum resource dedication (one task per core)

## Example Scenarios

### Scenario 1: Mixed Workload Cluster
```
Workers: 3 nodes with 8 CPU cores each (800 weight units each)
Connectors:
- FileConnector (weight=1, 400 tasks) → Distributed across all workers
- DatabaseConnector (weight=100, 6 tasks) → Gets dedicated cores
- APIConnector (weight=25, 12 tasks) → 4 tasks per core
```

### Scenario 2: High-Throughput Deployment
```
Workers: 5 nodes with 16 CPU cores each (1600 weight units each)
Connectors:
- StreamProcessor (weight=100, 10 tasks) → 2 tasks per worker
- DataAggregator (weight=20, 40 tasks) → Fills remaining capacity
- LogCollector (weight=5, 200 tasks) → High concurrency utilization
```

## Monitoring and Troubleshooting

### Key Metrics
- `assigned-tasks`: Number of tasks per worker
- `effective-cpu-load`: CPU utilization ratio (0.0-1.0+)
- `available-cpu-cores`: Total CPU cores per worker

### Common Issues
1. **Capacity Exhaustion**: Workers refuse high-weight tasks
   - **Solution**: Add more workers or reduce task weights
2. **Uneven Distribution**: Some workers idle while others overloaded
   - **Solution**: Review and adjust task weights
3. **Performance Degradation**: Tasks not getting expected resources
   - **Solution**: Increase task weight or reduce concurrent tasks

## Technical Architecture

### Class Hierarchy
```
ConnectorConfig
├── TASKS_WEIGHT_CONFIG (new)
├── TASKS_WEIGHT_DEFAULT = 1 (new)
└── tasksWeight() method (new)

WorkerLoad
├── availableCpuCores (new)
├── currentCpuLoad (new)
├── assign(task, weight) (new)
├── canAccommodateTask(weight) (new)
├── effectiveCpuLoad() (new)
└── capacityAwareTaskComparator() (new)

IncrementalCooperativeAssignor
├── assignTasks(workers, tasks, configSnapshot) (modified)
├── assignTasksCapacityAware() (new)
├── findBestWorkerForTask() (new)
└── getTaskWeight() (new)
```

### Integration Points
- **Configuration**: Extends existing ConnectorConfig with task weight
- **Assignment**: Enhances existing assignment algorithm without breaking changes  
- **Monitoring**: Integrates with existing Connect metrics framework
- **Compatibility**: Falls back to original behavior when weights not specified

## Future Enhancements

### Short Term
- Memory-aware allocation (tasks.memory configuration)
- Network bandwidth considerations
- Advanced scheduling policies

### Long Term  
- Machine learning-based resource prediction
- Dynamic weight adjustment based on runtime metrics
- Cross-cluster task migration for load balancing

This enhancement provides a foundation for intelligent resource management in Kafka Connect while maintaining full backward compatibility and operational simplicity.
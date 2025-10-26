<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Balanced Cooperative Task Assignment Algorithm

## Overview

This algorithm provides a balanced task distribution strategy for Kafka Connect in distributed mode using the cooperative rebalancing protocol. It is specifically designed for large-scale, multi-tenant, spot-instance environments where fast convergence and per-consumer fairness are critical.

**Design Goals:**

2. **Per-consumer fairness**: Each consumer (connector) must be balanced across all workers
3. **Global balance**: Total task load balanced across all workers
4. **Minimal disruption**: Preserves existing assignments when possible (scale-down, task additions)
5. **Autoscaling & Spot-instance Optimized**: Handles frequent worker scaling/churn efficiently

---

## Example Configuration

Throughout this document, we use the following example configuration:

| Parameter | Value |
|-----------|-------|
| **Total Workers** | 10 |
| **Total Consumers (Connectors)** | 9 |
| **Total Tasks** | 194 |

### Task Distribution by Consumer

| Consumer | Task Count | Tasks/Worker | perConsumerPerWorkerTaskCountMin | perConsumerPerWorkerTaskCountMax |
|----------|------------|--------------|----------------------------------|----------------------------------|
| c1 | 111 | 11.1 | 11 | 12 |
| c2 | 34 | 3.4 | 3 | 4 |
| c3 | 14 | 1.4 | 1 | 2 |
| c4 | 12 | 1.2 | 1 | 2 |
| c5 | 11 | 1.1 | 1 | 2 |
| c6 | 6 | 0.6 | 0 | 1 |
| c7 | 4 | 0.4 | 0 | 1 |
| c8 | 2 | 0.2 | 0 | 1 |
| c9 | 1 | 0.1 | 0 | 1 |

### Target Calculations

**Global Balance Targets:**
- `perWorkerTotalTasksMin` = floor(194/10) = **19** tasks per worker
- `perWorkerTotalTasksMax` = ceiling(194/10) = **20** tasks per worker
- `perWorkerTotalTasksMaxLimit` = perWorkerTotalTasksMax + 1 = **21** tasks per worker (tolerance threshold)

**Per-Consumer Balance Targets:**
- `perConsumerPerWorkerTaskCountMin` = floor(consumer_tasks / total_workers) - Minimum tasks each worker should have from consumer N
- `perConsumerPerWorkerTaskCountMax` = ceiling(consumer_tasks / total_workers) - Maximum tasks each worker can have from consumer N
- `maxNumberOfWorkersWithPerConsumerTaskCountMax` = (consumer_tasks - (perConsumerPerWorkerTaskCountMin × total_workers)) - Maximum number of workers in the cluster that should have perConsumerPerWorkerTaskCountMax from consumer N

**Objective:** Achieve balanced distribution where:
- Each worker has [perWorkerTotalTasksMin, perWorkerTotalTasksMax] total tasks
- For each consumer N, each worker has [perConsumerPerWorkerTaskCountMin, perConsumerPerWorkerTaskCountMax] tasks from that consumer
- At most `maxNumberOfWorkersWithPerConsumerTaskCountMax` workers can have perConsumerPerWorkerTaskCountMax tasks from consumer N
- The remaining workers have perConsumerPerWorkerTaskCountMin tasks from consumer N

---
---

## Algorithm Structure

The algorithm operates using incremental cooperative rebalancing, where each generation performs **either** revocations **or** assignments, never both. This follows the core principle of the cooperative protocol.

**Key Principle:** Just as we assign tasks 1-at-a-time to achieve balance, we also revoke tasks 1-at-a-time in the reverse direction.

### Cooperative Protocol Compliance

**CRITICAL**: Each rebalance generation executes in this sequence:

1. **Step 1 (ALWAYS)**: Calculate balance targets for current worker count
2. **Step 2 (ALWAYS)**: Check for balance violations (global and per-consumer)
3. **Step 3 (CONDITIONAL)**: Execute ONLY revocations if overload violations detected
4. **Step 4 (CONDITIONAL)**: Execute ONLY assignments if underload violations detected OR unassigned tasks exist

**Steps 3 and 4 are MUTUALLY EXCLUSIVE** - only one executes per generation, never both.

### Operation Modes

1. **Revocation Generation** (Step 3): Revokes tasks from overloaded workers when violations exceed thresholds
2. **Assignment Generation** (Step 4): Assigns unassigned tasks to underloaded workers or fills deficits

### Maximum Convergence Time
- **Balanced to balanced**: 0 generations (no rebalancing needed, Steps 1-2 detect no violations)
- **Minor imbalances**: 1-N generations (incremental revocations until balanced)
- **Scale-up scenarios**: N generations (incremental revocations, then assignments)
- **Scale-down scenarios**: 1-N generations (incremental assignments of lost tasks)
- **Task/connector changes**: 1-N generations (incremental adjustments)

Where N is proportional to the degree of imbalance - the further from balance, the more generations needed.

---

## Generation Execution Model

### Understanding the 4-Step Process

Every rebalance generation in the BalancedCooperativeAssignor follows a strict 4-step execution model that ensures compliance with the cooperative rebalancing protocol:

```
┌─────────────────────────────────────────────────────────────────┐
│                    EVERY GENERATION EXECUTES                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  STEP 1 (Always): Calculate Balance Targets                     │
│  ├─ Global: perWorkerTotalTasksMin/Max/MaxLimit                │
│  └─ Per-Consumer: perConsumerPerWorkerTaskCountMin/Max         │
│                                                                  │
│  STEP 2 (Always): Check for Balance Violations                  │
│  ├─ Global: overloadedWorkers, underloadedWorkers              │
│  ├─ Per-Consumer: consumerOverloads, consumerUnderloads        │
│  └─ Unassigned: unassignedTasks                                │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│              CONDITIONAL EXECUTION (NEVER BOTH)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  STEP 3 (If overload violations): REVOCATION GENERATION         │
│  └─ Revoke excess tasks from violated workers                  │
│  └─ Return revocations ONLY (no assignments)                   │
│                                                                  │
│            --- OR (MUTUALLY EXCLUSIVE) ---                      │
│                                                                  │
│  STEP 4 (If no overload violations): ASSIGNMENT GENERATION      │
│  └─ Assign tasks to fill deficits and unassigned              │
│  └─ Return assignments ONLY (no revocations)                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Cooperative Protocol Compliance

The key insight is that **Steps 3 and 4 are mutually exclusive** - a single generation executes EITHER Step 3 OR Step 4, but NEVER both:

| Violation State | Steps Executed | Result |
|----------------|----------------|--------|
| Overload violations detected | Steps 1, 2, **3** | Revocations only |
| No overload, but underload/unassigned | Steps 1, 2, **4** | Assignments only |
| No violations, no unassigned | Steps 1, 2 | No changes |

This design ensures:
- ✅ **Cooperative Protocol Compliance**: Never revoke AND assign in same generation
- ✅ **Prioritization**: Overload violations (Step 3) are fixed before assignments (Step 4)
- ✅ **Consistency**: Every generation checks balance (Steps 1-2) before acting
- ✅ **Incremental Progress**: Each generation makes targeted progress toward balance
- ✅ **Predictability**: Clear decision tree based on violation types

### Data Flow Through Generations

Understanding how data flows through the cooperative protocol:

```
Generation N starts:
  ↓
  IncrementalCooperativeAssignor.performAssignment() called
  ├─ Input: allMemberMetadata (current worker assignments)
  └─ Calls: BalancedCooperativeAssignor.performTaskAssignment()
      ↓
      STEP 1: Calculate balance targets based on current worker count
      STEP 2: Detect violations by comparing current state to targets
      ↓
      IF overload violations:
        STEP 3: Build revocation map (worker → tasks to revoke)
        Return: ClusterAssignment with revocations only
      ↓
      ELSE IF underload violations OR unassigned tasks:
        STEP 4: Build assignment map (worker → tasks to assign)
        Return: ClusterAssignment with assignments only
      ↓
      ELSE:
        Return: Empty ClusterAssignment (no changes)
  ↓
  Workers receive their assignments via onSyncGroupReceived()
  Workers apply changes (stop revoked tasks OR start new tasks)
  Workers rejoin cluster
  ↓
Generation N+1 starts with updated worker states...
```

### Key Variables Passed Between Steps

Understanding what data is available at each step:

**From Step 1 (Targets):**
```java
perWorkerTotalTasksMin          // floor(totalTasks / numWorkers)
perWorkerTotalTasksMax          // ceiling(totalTasks / numWorkers)
perWorkerTotalTasksMaxLimit     // perWorkerTotalTasksMax + 1

For each consumer:
  perConsumerPerWorkerTaskCountMin    // floor(consumerTasks / numWorkers)
  perConsumerPerWorkerTaskCountMax    // ceiling(consumerTasks / numWorkers)
  maxNumberOfWorkersWithPerConsumerTaskCountMax  // consumerTasks - (perConsumerPerWorkerTaskCountMin × numWorkers)
                                                  // Maximum workers allowed to have perConsumerPerWorkerTaskCountMax tasks
```

**From Step 2 (Violations):**
```java
overloadedWorkers         // List<WorkerState> with totalTasks > maxLimit
underloadedWorkers        // List<WorkerState> with totalTasks < min
consumerOverloads         // List<(WorkerState, Consumer)> exceeding max
consumerUnderloads        // List<(WorkerState, Consumer)> below min
unassignedTasks           // Set<ConnectorTaskId> not yet assigned
```

**To Step 3 (Revocation) OR Step 4 (Assignment):**
```java
// Both steps receive:
- Current worker states (from allMemberMetadata)
- Balance targets (from Step 1)
- Violation lists (from Step 2)

// Step 3 produces:
Map<String, Collection<ConnectorTaskId>> tasksToRevoke

// Step 4 produces:
Map<String, Collection<ConnectorTaskId>> tasksToAssign
```

---

## Incremental Rebalancing Logic with Intelligent Forecasting

**Purpose:** Incrementally achieve perfect balance by intelligently revoking from overloaded workers and assigning to underloaded workers, using forecasting to minimize total generations needed.

**Core Insight:** Before taking any action, forecast the target balanced state. Then revoke or assign one task per consumer per worker at a time, re-sorting after each action, until the forecasted balanced state is achieved.

### Enhanced Decision Tree for Each Generation

**IMPORTANT**: Steps 1 and 2 execute on EVERY generation. Steps 3 and 4 are mutually exclusive - only ONE executes per generation based on violation type detected in Step 2.

```
For each generation:
  ↓
  ═══════════════════════════════════════════════════════════════
  STEP 1 (ALWAYS EXECUTED): Calculate Balance Targets
  ═══════════════════════════════════════════════════════════════
  Calculate for current worker count:
    - perWorkerTotalTasksMin = floor(totalTasks / numWorkers)
    - perWorkerTotalTasksMax = ceiling(totalTasks / numWorkers)
    - perWorkerTotalTasksMaxLimit = perWorkerTotalTasksMax + 1
    - For each consumer:
        * perConsumerPerWorkerTaskCountMin = floor(consumerTasks / numWorkers)
        * perConsumerPerWorkerTaskCountMax = ceiling(consumerTasks / numWorkers)
        * maxNumberOfWorkersWithPerConsumerTaskCountMax = consumerTasks - (perConsumerPerWorkerTaskCountMin × numWorkers)
          (This is the maximum number of workers that should have perConsumerPerWorkerTaskCountMax tasks from this consumer)
  ↓
  ═══════════════════════════════════════════════════════════════
  STEP 2 (ALWAYS EXECUTED): Check for Balance Violations
  ═══════════════════════════════════════════════════════════════
  Check TWO types of violations:
     
     A. Global Balance Violation Check:
        overloadedWorkers = []
        underloadedWorkers = []
        
        FOR each worker:
          IF worker.totalTasks > perWorkerTotalTasksMaxLimit:
            overloadedWorkers.add(worker)
          ELSE IF worker.totalTasks < perWorkerTotalTasksMin:
            underloadedWorkers.add(worker)
     
     B. Per-Consumer Balance Violation Check:
        consumerOverloads = []   # (worker, consumer) pairs
        consumerUnderloads = []  # (worker, consumer) pairs
        
        FOR each worker:
          FOR each consumer on worker:
            # Check if worker exceeds max for this consumer
            IF worker.consumerTaskCount[consumer] > perConsumerPerWorkerTaskCountMax[consumer]:
              consumerOverloads.add((worker, consumer))
            
            # Check if too many workers have max count for this consumer
            ELSE IF worker.consumerTaskCount[consumer] == perConsumerPerWorkerTaskCountMax[consumer]:
              # Count how many workers currently have max count for this consumer
              workersWithMaxCount = countWorkersWithTaskCount(consumer, perConsumerPerWorkerTaskCountMax[consumer])
              
              IF workersWithMaxCount > maxNumberOfWorkersWithPerConsumerTaskCountMax[consumer]:
                # Too many workers have max count - mark excess workers for revocation
                # Select workers to revoke from (e.g., by total load or round-robin)
                consumerOverloads.add((worker, consumer))
            
            # Check if worker is below min for this consumer
            ELSE IF worker.consumerTaskCount[consumer] < perConsumerPerWorkerTaskCountMin[consumer]:
              consumerUnderloads.add((worker, consumer))
     
     A. Global Balance Violation Check:
        overloadedWorkers = []
        underloadedWorkers = []
        
        FOR each worker:
          IF worker.totalTasks > perWorkerTotalTasksMaxLimit:
            → OVERLOAD VIOLATION: Add to overloadedWorkers
            
          IF worker.totalTasks < perWorkerTotalTasksMin:
            → UNDERLOAD VIOLATION: Add to underloadedWorkers
     
     B. Per-Consumer Balance Violation Check:
        consumerOverloads = []   # (worker, consumer) pairs
        consumerUnderloads = []  # (worker, consumer) pairs
        
        FOR each worker:
          FOR each consumer on worker:
            IF worker.consumerTaskCount[consumer] > perConsumerPerWorkerTaskCountMax[consumer]:
              → OVERLOAD VIOLATION: Add (worker, consumer) to consumerOverloads
              
            IF worker.consumerTaskCount[consumer] < perConsumerPerWorkerTaskCountMin[consumer]:
              → UNDERLOAD VIOLATION: Add (worker, consumer) to consumerUnderloads
     
     C. Check for Unassigned Tasks:
        unassignedTasks = getAllUnassignedTasks()
     
     IF overloadedWorkers.isEmpty() AND consumerOverloads.isEmpty() 
        AND underloadedWorkers.isEmpty() AND consumerUnderloads.isEmpty()
        AND unassignedTasks.isEmpty():
       → NO ACTION: Cluster is perfectly balanced ✅
       RETURN empty assignments and revocations
       Workers continue with current assignments
  ↓
  ═══════════════════════════════════════════════════════════════
  STEP 3 (CONDITIONAL - ONLY if overload violations detected):
  REVOCATION GENERATION
  ═══════════════════════════════════════════════════════════════
     IF overloadedWorkers NOT empty OR consumerOverloads NOT empty:
       → EXECUTE REVOCATION GENERATION:
          
          Purpose: Remove ALL excess tasks from overloaded workers in this generation
          
          Action A: Revoke from per-consumer overloads
            FOR each (worker, consumer) in consumerOverloads:
              excessCount = worker.consumerTaskCount[consumer] - perConsumerPerWorkerTaskCountMin[consumer]
              Revoke ALL excessCount tasks of 'consumer' from 'worker'
              Update worker state immediately
          
          Action B: Revoke from globally overloaded workers
            FOR each worker in overloadedWorkers:
              WHILE worker.totalTasks > perWorkerTotalTasksMin:
                Select consumer with most tasks on worker
                Revoke 1 task of that consumer from worker
                Update worker state immediately
                Re-check if worker still in overloadedWorkers list
              # This continues until worker.totalTasks <= perWorkerTotalTasksMin
          
          Return: revocations ONLY (no assignments in this generation)
          
          Workers will:
            - Stop ALL revoked tasks
            - Rejoin cluster
            - Next generation will re-run Steps 1-2 and proceed based on violations
  ↓
  ═══════════════════════════════════════════════════════════════
  STEP 4 (CONDITIONAL - ONLY if no overload violations):
  ASSIGNMENT GENERATION
  ═══════════════════════════════════════════════════════════════
     ELSE IF underloadedWorkers NOT empty OR consumerUnderloads NOT empty OR unassignedTasks NOT empty:
       → EXECUTE ASSIGNMENT GENERATION:
          
          Purpose: Fill underloaded workers and assign unassigned tasks
          
          Phase A: Fill to minimum targets (skip if they are already met)
            FOR each (worker, consumer) in consumerUnderloads:
              deficitCount = perConsumerPerWorkerTaskCountMin[consumer] - worker.consumerTaskCount[consumer]
              Assign deficitCount tasks of 'consumer' to 'worker' from unassigned pool
              Update worker state immediately
            
            FOR each worker in underloadedWorkers: (skip if they are already met)
              WHILE worker.totalTasks < perWorkerTotalTasksMin AND unassigned exist:
                Select consumer with fewest tasks on worker
                Assign 1 task of that consumer to worker
                Update worker state immediately
          
          Phase B: Distribute remaining unassigned tasks
            WHILE unassignedTasks NOT empty:
              Select worker with lowest totalTasks (≤ perWorkerTotalTasksMax)
              Select consumer with fewest tasks on that worker
              Assign 1 task of consumer to worker
              Update worker state immediately
          
          Return: assignments ONLY (no revocations in this generation)
          
          Workers will:
            - Start newly assigned tasks
            - Continue running
            - Next generation will re-run Steps 1-2 and proceed based on violations
  ↓
  ═══════════════════════════════════════════════════════════════
  GENERATION COMPLETE
  ═══════════════════════════════════════════════════════════════
  Workers apply changes (EITHER revocations OR assignments, never both)
  Workers rejoin cluster for next generation
  Next generation repeats from Step 1
```

### Intelligent Forecasting Strategy

Before any revocation or assignment, the algorithm:

1. **Calculates Target State:**
   - For each worker: target total tasks in [perWorkerTotalTasksMin, perWorkerTotalTasksMax]
   - For each consumer: target tasks per worker in [perConsumerPerWorkerTaskCountMin, perConsumerPerWorkerTaskCountMax]
   - For each consumer: calculate `maxNumberOfWorkersWithPerConsumerTaskCountMax` to limit how many workers can have the maximum count

2. **Projects Revocation Path:**
   - Identifies which tasks need to move from which workers
   - Prioritizes consumers that are furthest from balance
   - Plans minimal revocations to reach target state
   - **NEW**: Checks if too many workers have `perConsumerPerWorkerTaskCountMax` for any consumer

3. **Validates Assignment Path:**
   - Before assigning, forecasts if assignment moves toward balance
   - Checks if assignment violates perWorkerTotalTasksMaxLimit or perConsumerPerWorkerTaskCountMax
   - **NEW**: Checks if assignment would cause too many workers to have perConsumerPerWorkerTaskCountMax
   - Only assigns if it improves balance metrics

4. **Re-sorts After Each Action:**
   - After each single task revocation/assignment
   - Re-evaluates worker loads across all consumers
   - Ensures fairness and balance throughout the process

### Why Incremental Works Better

| Aspect | Full Revocation (Old) | Incremental (New) |
|--------|----------------------|-------------------|
| **Disruption** | High - revokes ALL tasks | Minimal - revokes 1 task at a time |
| **Convergence** | 2 generations (all revoke, then all assign) | N generations (gradual approach) |
| **Cloud-native fit** | Poor - large disruption on spot termination | Excellent - graceful degradation |
| **Protocol compliance** | Basic - uses 2 generations | Natural - follows cooperative philosophy |
| **Fairness** | Eventual - only balanced after 2 rounds | Continuous - improves every generation |

---

## Revocation Generation with Intelligent Forecasting

**Execution Context**: This is Step 3 in the generation cycle, executed ONLY when Step 2 detects overload violations.

**Trigger Conditions (from Step 2):** 
- `overloadedWorkers` list is NOT empty (workers with totalTasks > perWorkerTotalTasksMaxLimit)
- OR `consumerOverloads` list is NOT empty (worker-consumer pairs exceeding perConsumerPerWorkerTaskCountMax)

**Operation:** Revoke ALL excess tasks that violate limits, update worker state, return revocations only

### Algorithm Flow (Step 3 - Revocation Generation)

**Precondition**: Steps 1 and 2 have already executed, and overload violations were detected.

```
═══════════════════════════════════════════════════════════════
STEP 3: REVOCATION GENERATION (Only if overload violations)
═══════════════════════════════════════════════════════════════

GIVEN (from Step 2):
  - overloadedWorkers = workers with totalTasks > perWorkerTotalTasksMaxLimit
  - consumerOverloads = (worker, consumer) pairs with violations:
      * worker.consumerTaskCount[consumer] > perConsumerPerWorkerTaskCountMax[consumer]
      * OR worker has perConsumerPerWorkerTaskCountMax[consumer] tasks but total workers 
        with this count > maxNumberOfWorkersWithPerConsumerTaskCountMax[consumer]
  - Balance targets calculated in Step 1 (including maxNumberOfWorkersWithPerConsumerTaskCountMax for each consumer)

1. Initialize revocation tracking:
   revocations = empty map of (worker → list of tasks to revoke)
   
2. Revoke from per-consumer violations FIRST:
   
   # Sort by violation severity (most overloaded first)
   sortedConsumerViolations = sort consumerOverloads by:
     (actual_count - perConsumerPerWorkerTaskCountMin) descending
   
   FOR each (worker, consumer) in sortedConsumerViolations:
     # Calculate exact excess beyond minimum threshold
     excessCount = worker.consumerTaskCount[consumer] - perConsumerPerWorkerTaskCountMin[consumer]
     
     # Revoke ALL excess tasks from this consumer on this worker
     FOR i = 1 to excessCount:
       task = worker.selectTaskFromConsumer(consumer)
       revocations[worker].add(task)
       
       # Update worker state immediately for next iteration
       worker.consumerTaskCount[consumer] -= 1
       worker.totalTasks -= 1
       
       LOG: "Revoking {task} from {worker} (consumer overload: {actual} > {min})"
   
3. Revoke from globally overloaded workers:
   
   # Some workers may still be overloaded globally even after per-consumer fixes
   FOR each worker in overloadedWorkers:
     WHILE worker.totalTasks > perWorkerTotalTasksMin:
       # Select consumer with most tasks on this worker (spread the revocation)
       consumer = selectConsumerWithMostTasksOnWorker(worker)
       task = worker.selectTaskFromConsumer(consumer)
       
       revocations[worker].add(task)
       
       # Update worker state immediately
       worker.consumerTaskCount[consumer] -= 1
       worker.totalTasks -= 1
       
       LOG: "Revoking {task} from {worker} (global overload: {totalTasks} > {min})"
     # This continues until worker.totalTasks <= perWorkerTotalTasksMin

4. Return revocation assignments:
   
   Return ClusterAssignment with:
     - newlyAssignedConnectors: empty
     - newlyAssignedTasks: empty
     - newlyRevokedConnectors: (as needed)
     - newlyRevokedTasks: revocations
     - allAssignedConnectors: (unchanged from current)
     - allAssignedTasks: (current minus revocations)

5. Workers apply revocations and rejoin:
   
   Each worker receives assignment with:
     - Tasks to stop: revocations[worker]
     - Tasks to start: (none - this is revocation-only generation)
   
   Workers will:
     - Stop and cleanup revoked tasks
     - Flush offsets and state
     - Rejoin cluster for next generation
     
   Next generation will:
     - Re-run Steps 1-2 (calculate targets and check violations)
     - If still overloaded: repeat Step 3 (more revocations)
     - If no longer overloaded but underloaded/unassigned: execute Step 4 (assignments)
     - If balanced: no action

═══════════════════════════════════════════════════════════════
END OF STEP 3
═══════════════════════════════════════════════════════════════
```

### Key Enhancements

1. **Balance Check First:** Always verify if rebalancing is needed before taking action

2. **Forecasting:** Calculate target state before revoking to know when to stop

3. **Consumer-First Approach:** Revoke 1 task per consumer per worker (not just 1 task total)
   - Ensures per-consumer balance improves every generation
   - Prevents over-concentration on single consumer

4. **Intelligent Sorting:** Re-sort after each revocation
   - Maintains fairness across all workers
   - Ensures next revocation targets most overloaded worker

5. **Convergence Check:** After each revocation, check if consumer is now balanced
   - Avoids unnecessary revocations
   - Moves to next consumer once current is balanced

### Revocation Priority Logic

When selecting which task to revoke from a worker:

```
Priority order:
1. Consumer with highest (actual_count - perConsumerPerWorkerTaskCountMax)
   → Most overloaded consumer first
   
2. Within consumer, worker with highest consumerTaskCount
   → Most loaded worker for that consumer first
   
3. Re-sort after each revocation
   → Ensures fairness maintained throughout
```

### Revocation Example with Intelligent Forecasting

**Scenario:** 10 workers, 194 tasks, worker w0 has 25 tasks (globalMax = 20), with per-consumer imbalance

**Initial State:**
```
w0: 25 tasks total (15 c1, 6 c2, 2 c3, 2 c4) - OVERLOADED
w1-w9: 19 tasks each (11 c1, 3 c2, 1 c3, 2 c4, 1 c5)

Targets:
  perWorkerTotalTasksMax = 20
  c1: max = 12
  c2: max = 4
```

**Generation 1 - Violation Detection and Revocation:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  perWorkerTotalTasksMin = floor(194/10) = 19
  perWorkerTotalTasksMax = ceiling(194/10) = 20
  perWorkerTotalTasksMaxLimit = 21
  
  Per-consumer targets:
    c1: min=11, max=12
    c2: min=3, max=4

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Global violations:
    w0.totalTasks = 25 > perWorkerTotalTasksMaxLimit(21) → OVERLOAD ❌
    overloadedWorkers = [w0]
  
  Per-consumer violations:
    w0.c1Count = 15 > c1max(12) → OVERLOAD ❌
    w0.c2Count = 6 > c2max(4) → OVERLOAD ❌
    consumerOverloads = [(w0, c1), (w0, c2)]
  
  Decision: OVERLOAD VIOLATIONS DETECTED → Execute Step 3 (Revocation)

═══════════════════════════════════════════════════════════════
STEP 3: REVOCATION GENERATION
═══════════════════════════════════════════════════════════════
  Forecast Target State:
    w0 should have: 20 total (12 c1, 4 c2)
    Need to revoke: 5 tasks total (3 c1, 2 c2)
  
  Action: Revoke from per-consumer overloads first
    c1 excess: 15 - 12 = 3 tasks to revoke
    c2 excess: 6 - 4 = 2 tasks to revoke
  
  Revocations for Generation 1:
    w0 → [c1-task-42, c1-task-43, c1-task-44, c2-task-15, c2-task-16]
  
  Result:
    - Revocations: 5 tasks from w0
    - Assignments: NONE (this is revocation-only generation)
    - New state after revocation: w0=20 tasks (12 c1, 4 c2), 5 unassigned
    
  Workers rejoin and trigger Generation 2...
```

**Generation 2 - Balanced Check and Assignment:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  perWorkerTotalTasksMin = 19
  perWorkerTotalTasksMax = 20
  perWorkerTotalTasksMaxLimit = 21
  (Targets unchanged - same worker count)

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Global violations:
    All workers: 19-20 tasks → Within [19, 21] ✅
    overloadedWorkers = [] (empty)
  
  Per-consumer violations:
    All workers within [min, max] for each consumer ✅
    consumerOverloads = [] (empty)
  
  Unassigned tasks:
    5 tasks unassigned (from previous revocation)
    unassignedTasks = [c1-task-42, c1-task-43, c1-task-44, c2-task-15, c2-task-16]
  
  Decision: NO OVERLOAD, BUT UNASSIGNED TASKS → Execute Step 4 (Assignment)

═══════════════════════════════════════════════════════════════
STEP 4: ASSIGNMENT GENERATION
═══════════════════════════════════════════════════════════════
  Phase A: No deficits (all workers ≥ min)
  
  Phase B: Distribute 5 unassigned tasks
    Assign to workers with lowest total (currently 19 tasks):
      w1 → c1-task-42 (now 20 tasks)
      w2 → c1-task-43 (now 20 tasks)
      w3 → c1-task-44 (now 20 tasks)
      w4 → c2-task-15 (now 20 tasks)
      w5 → c2-task-16 (now 20 tasks)
  
  Result:
    - Revocations: NONE (this is assignment-only generation)
    - Assignments: 5 tasks to w1-w5
    - Final state: All workers 19-20 tasks, perfectly balanced ✅
```

**Final:** All workers at 19-20 tasks, per-consumer balanced

**Key Observations:**
- **Generation 1**: Steps 1-2 detected overload violations → Step 3 executed (revocations only)
- **Generation 2**: Steps 1-2 detected no overload but unassigned tasks → Step 4 executed (assignments only)
- **NEVER** revoked and assigned in the same generation
- Total: 2 generations (1 revocation + 1 assignment)

---

### Example: maxNumberOfWorkersWithPerConsumerTaskCountMax Constraint

**Scenario:** Consumer c9 with 1 task, 10 workers

**Target Calculations:**
```
c9: 1 task total
perConsumerPerWorkerTaskCountMin = floor(1/10) = 0
perConsumerPerWorkerTaskCountMax = ceiling(1/10) = 1
maxNumberOfWorkersWithPerConsumerTaskCountMax = 1 - (0 × 10) = 1

This means: Only 1 worker should have 1 task from c9, the other 9 workers should have 0 tasks
```

**Violation Case (before fix):**
```
Initial state: 3 workers have 1 c9 task each (w0, w1, w2)
Workers with max (1 task): 3
maxNumberOfWorkersWithPerConsumerTaskCountMax: 1
Violation: 3 > 1 ❌ (too many workers have the maximum)
```

**Generation N - Detection and Revocation:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  c9: min=0, max=1
  maxNumberOfWorkersWithPerConsumerTaskCountMax = 1
  (Only 1 worker should have 1 c9 task)

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Current c9 distribution:
    w0: 1 c9 task
    w1: 1 c9 task
    w2: 1 c9 task
    w3-w9: 0 c9 tasks
  
  Per-consumer violation check:
    workersWithMaxCount = 3 (w0, w1, w2 all have 1 task)
    maxNumberOfWorkersWithPerConsumerTaskCountMax[c9] = 1
    
    3 > 1 → VIOLATION ❌
    
  Decision: Mark (w1, c9) and (w2, c9) for revocation
            (Keep w0 with the task, revoke from w1 and w2)

═══════════════════════════════════════════════════════════════
STEP 3: REVOCATION GENERATION
═══════════════════════════════════════════════════════════════
  Revoke 1 task from w1 (c9)
  Revoke 1 task from w2 (c9)
  
  Result:
    w0: 1 c9 task ✅
    w1: 0 c9 tasks (task revoked)
    w2: 0 c9 tasks (task revoked)
    w3-w9: 0 c9 tasks
    
    workersWithMaxCount = 1 (only w0 has 1 task)
    Balanced! ✅
```

**Why This Matters:**

Without this constraint, the algorithm could incorrectly distribute small consumers across multiple workers:
- ❌ **Without constraint**: c9's 1 task could end up on any worker, causing imbalance
- ✅ **With constraint**: c9's 1 task stays on exactly 1 worker, maintaining perfect distribution

This is especially important for small consumers (tasks ≤ workers) where task count doesn't evenly divide across all workers.

### What Triggers Revocation Generation

Revocation generation is triggered if **ANY** of these balance violations are detected:

#### 1. Global Overload Violation
```
FOR each worker:
  IF worker.totalTasks > perWorkerTotalTasksMaxLimit:
    → REVOCATION GENERATION
```

**Why:** Worker is overloaded beyond the tolerance threshold and must shed ALL excess tasks down to perWorkerTotalTasksMin

**Scenarios:**
- Scale-up: Existing workers have more than new perWorkerTotalTasksMaxLimit
- Failed rebalances: Some workers accumulated too many tasks
- Manual errors: Incorrect assignments
- Post-connector-deletion: Remaining tasks push worker over limit

**Action:** Revoke tasks one at a time until worker.totalTasks <= perWorkerTotalTasksMin

#### 2. Per-Consumer Overload Violation
```
FOR each worker:
  FOR each consumer on worker:
    IF worker.consumerTaskCount[consumer] > perConsumerPerWorkerTaskCountMin[consumer]:
      → REVOCATION GENERATION
```

**Why:** Worker has too many tasks from a specific consumer and must shed ALL excess tasks down to perConsumerPerWorkerTaskCountMin for fairness

**Scenarios:**
- Connector task count increased, pushing existing workers over per-consumer min
- Previous generation didn't fully balance per-consumer distribution
- Scale-up reduced per-consumer max, but existing workers exceed new limit

#### 3. Per-Consumer Distribution Violation (NEW)
```
FOR each consumer:
  workersWithMaxCount = count of workers with perConsumerPerWorkerTaskCountMax tasks from this consumer
  
  IF workersWithMaxCount > maxNumberOfWorkersWithPerConsumerTaskCountMax:
    → REVOCATION GENERATION
```

**Why:** Too many workers have the maximum task count for this consumer, violating the balanced distribution constraint

**Scenarios:**
- Small consumers (tasks ≤ workers): Ensures only the exact number of workers needed have tasks
  - Example: c9 with 1 task, 10 workers → only 1 worker should have 1 task, not multiple workers
- After scale-down: Remaining workers may all have max count when only some should
- Previous generation over-distributed max tasks before checking constraint

**Example:**
```
Consumer c2: 34 tasks, 10 workers
  perConsumerPerWorkerTaskCountMin = 3
  perConsumerPerWorkerTaskCountMax = 4
  maxNumberOfWorkersWithPerConsumerTaskCountMax = 34 - (3 × 10) = 4
  
  Correct distribution: 4 workers with 4 tasks, 6 workers with 3 tasks
  Violation: If 5+ workers have 4 tasks → triggers revocation
```

This constraint ensures mathematically perfect distribution where exactly the right number of workers have the maximum count.

#### Critical Distinction: Violation-Based vs Traditional Triggers

**OLD Approach (Round 1 Detection):**
- Trigger: Empty worker detected (any worker has 0 tasks)
- Action: Revoke ALL tasks from ALL workers

**NEW Approach (Violation-Based):**
- Trigger: Specific overload violations detected
- Action: Revoke only from violating workers/consumers
- Benefit: No unnecessary full revocations

### What Does NOT Trigger Revocation Generation

Revocation is **NOT** triggered for:
- ✅ Worker scale-down (departed workers) → No overload violations, go directly to Assignment Generation
- ✅ New tasks added to existing connector → No overload violations, go directly to Assignment Generation  
- ✅ Connector removal (deleted tasks filtered out) → May trigger Assignment if rebalance needed
- ✅ Workers within limits but below min → Underload violations only, go to Assignment Generation
- ✅ Empty workers (new workers with 0 tasks) → Underload violations only, go to Assignment Generation

**Key Principle:** Revocation is ONLY triggered when workers or consumers are **overloaded** (exceed max limits). 
Underloaded workers (below min) are handled by Assignment Generation, not Revocation Generation.

These scenarios have no overload violations, so they proceed directly to Assignment Generation.

---

## Assignment Generation with Intelligent Forecasting

**Execution Context**: This is Step 4 in the generation cycle, executed ONLY when Step 2 detects NO overload violations AND (underload violations exist OR unassigned tasks exist).

**Trigger Conditions (from Step 2):**
- `overloadedWorkers` list IS empty AND `consumerOverloads` list IS empty (no overload violations)
- AND at least ONE of:
  - `underloadedWorkers` list is NOT empty (workers with totalTasks < perWorkerTotalTasksMin)
  - OR `consumerUnderloads` list is NOT empty (worker-consumer pairs below perConsumerPerWorkerTaskCountMin)
  - OR `unassignedTasks` set is NOT empty (tasks not yet assigned to any worker)

**Operation:** Fill deficits to minimum targets, then distribute remaining unassigned tasks, return assignments only

### Algorithm Flow (Step 4 - Assignment Generation)

**Precondition**: Steps 1 and 2 have already executed, NO overload violations detected, but underload violations or unassigned tasks exist.

```
═══════════════════════════════════════════════════════════════
STEP 4: ASSIGNMENT GENERATION (Only if no overload violations)
═══════════════════════════════════════════════════════════════

GIVEN (from Step 2):
  - underloadedWorkers = workers with totalTasks < perWorkerTotalTasksMin
  - consumerUnderloads = (worker, consumer) pairs with count < perConsumerPerWorkerTaskCountMin
  - unassignedTasks = tasks not assigned to any worker
  - Balance targets calculated in Step 1
  
PRECONDITION CHECK:
  overloadedWorkers IS empty ✅
  consumerOverloads IS empty ✅
  (This ensures we only assign, never revoke in this generation)

1. Initialize assignment tracking:
   assignments = empty map of (worker → list of tasks to assign)
   
2. PHASE A: Fill workers to minimum targets first
   -------------------------------------------------------
   
   2a. Fill per-consumer deficits:
       
       # Sort by deficit severity (most underloaded first)
       sortedConsumerUnderloads = sort consumerUnderloads by:
         (perConsumerPerWorkerTaskCountMin - actual_count) descending
       
       FOR each (worker, consumer) in sortedConsumerUnderloads:
         # Calculate exact deficit
         deficitCount = perConsumerPerWorkerTaskCountMin[consumer] - worker.consumerTaskCount[consumer]
         
         # Assign up to deficitCount tasks from unassigned pool
         tasksAvailable = unassignedTasks[consumer].size()
         tasksToAssign = min(deficitCount, tasksAvailable)
         
         FOR i = 1 to tasksToAssign:
           task = unassignedTasks[consumer].removeFirst()
           assignments[worker].add(task)
           
           # Update worker state immediately for next iteration
           worker.consumerTaskCount[consumer] += 1
           worker.totalTasks += 1
           
           LOG: "Assigning {task} to {worker} (consumer deficit: {actual} < {min})"
   
   2b. Fill globally underloaded workers:
       
       FOR each worker in underloadedWorkers:
         WHILE worker.totalTasks < perWorkerTotalTasksMin AND unassignedTasks NOT empty:
           # Select consumer with fewest tasks on this worker (spread evenly)
           consumer = selectConsumerWithFewestTasksOnWorker(worker)
           
           IF unassignedTasks[consumer].isEmpty():
             CONTINUE to next consumer
           
           task = unassignedTasks[consumer].removeFirst()
           assignments[worker].add(task)
           
           # Update worker state immediately
           worker.consumerTaskCount[consumer] += 1
           worker.totalTasks += 1
           
           LOG: "Assigning {task} to {worker} (global deficit: {totalTasks} < {min})"

3. PHASE B: Distribute remaining unassigned tasks
   -------------------------------------------------------
   
   WHILE unassignedTasks NOT empty:
     # Find worker with lowest total tasks that's still below max
     eligibleWorkers = workers where totalTasks ≤ perWorkerTotalTasksMax
     
     IF eligibleWorkers.isEmpty():
       # All workers at max, but tasks remain
       # This can happen when totalTasks > workers × perWorkerTotalTasksMax
       LOG: "WARNING: All workers at max capacity, but {unassignedTasks.size()} tasks remain"
       
       # Force assignment to worker with fewest total tasks
       worker = selectWorkerWithFewestTotalTasks()
       LOG: "Force assigning to {worker} beyond max (totalTasks will be {worker.totalTasks + 1})"
     ELSE:
       worker = selectWorkerWithLowestTotalTasksFromEligible(eligibleWorkers)
     
     # Select consumer with fewest tasks on this worker (balance across consumers)
     consumer = selectConsumerWithFewestTasksOnWorker(worker)
     
     IF unassignedTasks[consumer].isEmpty():
       # This consumer has no more tasks, try next consumer
       CONTINUE to next consumer with unassigned tasks
     
     # NEW: Check if assigning would violate maxNumberOfWorkersWithPerConsumerTaskCountMax
     IF worker.consumerTaskCount[consumer] + 1 == perConsumerPerWorkerTaskCountMax[consumer]:
       # This assignment would bring worker to max for this consumer
       workersWithMaxCount = countWorkersWithTaskCount(consumer, perConsumerPerWorkerTaskCountMax[consumer])
       
       IF workersWithMaxCount >= maxNumberOfWorkersWithPerConsumerTaskCountMax[consumer]:
         # Too many workers already have max - skip this worker for this consumer
         LOG: "Skipping {worker} for {consumer} - would exceed maxNumberOfWorkersWithPerConsumerTaskCountMax"
         CONTINUE to next eligible worker
     
     task = unassignedTasks[consumer].removeFirst()
     assignments[worker].add(task)
     
     # Update worker state immediately
     worker.consumerTaskCount[consumer] += 1
     worker.totalTasks += 1
     
     LOG: "Assigning {task} to {worker} (distributing remaining: worker now has {totalTasks})"

4. Return assignment:
   
   Return ClusterAssignment with:
     - newlyAssignedConnectors: (as computed)
     - newlyAssignedTasks: assignments
     - newlyRevokedConnectors: empty
     - newlyRevokedTasks: empty
     - allAssignedConnectors: (current plus new assignments)
     - allAssignedTasks: (current plus new assignments)

5. Workers apply assignments and continue:
   
   Each worker receives assignment with:
     - Tasks to start: assignments[worker]
     - Tasks to stop: (none - this is assignment-only generation)
   
   Workers will:
     - Start newly assigned tasks
     - Continue running existing tasks
     - No task disruption
     
   Next generation will:
     - Re-run Steps 1-2 (calculate targets and check violations)
     - If still underloaded/unassigned: repeat Step 4 (more assignments)
     - If overloaded: execute Step 3 (revocations)
     - If balanced: no action

═══════════════════════════════════════════════════════════════
END OF STEP 4
═══════════════════════════════════════════════════════════════
```

### Key Enhancements

1. **Violation-Based Triggering:** Assignment is triggered ONLY when violations detected
   - Checks for underload violations (workers below perWorkerTotalTasksMin)
   - Checks for per-consumer underload violations (below perConsumerPerWorkerTaskCountMin)
   - Checks for unassigned tasks in the cluster
   - No action if no violations and no unassigned tasks

2. **Two-Phase Assignment:**
   - **Phase A:** Fill underloaded workers/consumers to minimum targets
   - **Phase B:** Distribute remaining unassigned tasks evenly across workers

3. **Direct Deficit Filling:** Instead of 1 task per iteration, fills deficits directly
   - Calculate exact deficit: `perConsumerPerWorkerTaskCountMin - actual_count`
   - Assign multiple tasks at once to reach minimum targets
   - More efficient than gradual incremental approach

4. **Even Distribution:** Phase B assigns remaining tasks fairly
   - Always assigns to worker with lowest total tasks (below max)
   - Spreads tasks from each consumer evenly across workers
   - Prevents over-concentration on single worker

5. **Respects Limits:** Never violates maximum thresholds during assignment
   - Global check: `worker.totalTasks ≤ perWorkerTotalTasksMax`
   - Per-consumer check: `worker.consumerTaskCount ≤ perConsumerPerWorkerTaskCountMax`
   - **NEW**: Per-consumer distribution check: Ensure workers with `perConsumerPerWorkerTaskCountMax` don't exceed `maxNumberOfWorkersWithPerConsumerTaskCountMax`
   - Falls back to force assignment only if all workers at capacity

### Assignment Priority Logic

When selecting which worker to assign to:

```
Priority order:
1. Workers below perConsumerPerWorkerTaskCountMin for the consumer
   → Fill minimum guarantees first
   
2. Among eligible workers, choose worker with lowest totalTasks
   → Maintain global balance
   
3. Re-sort after each assignment
   → Ensures continuous balance improvement
   
4. Validate against forecasted target state
   → Only assign if it moves toward balance
```

### Assignment Priority Strategy

The assignment logic prioritizes:

1. **Per-Consumer Targets First:** Fill workers to `perConsumerPerWorkerTaskCountMin` for each consumer
2. **Global Balance:** Always assign to least-loaded worker (by total task count)
3. **Fairness:** Rotate through consumers to avoid over-assigning one consumer
4. **Small Consumer Protection:** Limit small connectors (tasks ≤ workers) to max 1 task per worker

### Assignment Example: Scale-Down

**Scenario:** 10 workers → 8 workers (w8, w9 terminated), 38 tasks lost

**After Delayed Rebalance (30 seconds):**

**Generation N - Violation Detection and Assignment:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  NEW worker count: 8 (down from 10)
  Total tasks: 194 (unchanged)
  
  perWorkerTotalTasksMin = floor(194/8) = 24
  perWorkerTotalTasksMax = ceiling(194/8) = 25
  perWorkerTotalTasksMaxLimit = 26
  
  Per-consumer targets (updated for 8 workers):
    c1: min=13, max=14 (was 11, 12 for 10 workers)
    c2: min=4, max=5 (was 3, 4 for 10 workers)

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Current state:
    w0-w7: each has 19-20 tasks (their previous assignments)
    38 tasks unassigned (lost from w8, w9)
  
  Global violations:
    All workers: 19-20 tasks < perWorkerTotalTasksMin(24) → UNDERLOAD ❌
    underloadedWorkers = [w0, w1, w2, w3, w4, w5, w6, w7] (all 8 workers)
    overloadedWorkers = [] (empty - no overload)
  
  Unassigned tasks:
    38 tasks from departed workers
    unassignedTasks.size() = 38
  
  Decision: NO OVERLOAD, BUT UNDERLOAD + UNASSIGNED → Execute Step 4 (Assignment)

═══════════════════════════════════════════════════════════════
STEP 4: ASSIGNMENT GENERATION
═══════════════════════════════════════════════════════════════
  Phase A: Fill workers to minimum (24 tasks per worker)
    Each of 8 workers needs: 24 - 20 = 4-5 more tasks
  
  Phase B: Distribute 38 unassigned tasks across 8 workers
    Round 1: Assign 1 task to each of 8 workers (8 tasks assigned, 30 remain)
  
  Result for Generation N:
    - Revocations: NONE (this is assignment-only generation)
    - Assignments: 8 tasks (1 to each worker)
      w0 → c1-task-X, w1 → c1-task-Y, ..., w7 → c2-task-Z
    - New state: w0-w7 now have 20-21 tasks each, 30 unassigned remain
    
  Workers continue running and trigger Generation N+1...
```

**Generation N+1 - Continue Assignment:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  Same as Generation N (worker count unchanged)
  perWorkerTotalTasksMin = 24
  perWorkerTotalTasksMax = 25

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Current state:
    w0-w7: now have 20-21 tasks each
    30 tasks still unassigned
  
  Global violations:
    All workers: 20-21 < min(24) → Still UNDERLOAD ❌
    underloadedWorkers = [w0, w1, w2, w3, w4, w5, w6, w7]
  
  Decision: NO OVERLOAD, BUT UNDERLOAD + UNASSIGNED → Execute Step 4 (Assignment)

═══════════════════════════════════════════════════════════════
STEP 4: ASSIGNMENT GENERATION
═══════════════════════════════════════════════════════════════
  Assign 1 task to each of 8 workers
  
  Result:
    - Assignments: 8 tasks (1 to each worker)
    - New state: w0-w7 now have 21-22 tasks, 22 unassigned remain
```

**Generation N+2 through N+4:**
```
Each generation:
  - Steps 1-2: Calculate targets and detect underload violations
  - Step 4: Assign 8 more tasks (1 per worker)
Continue incrementally...
```

**Generation N+5 - Final Assignment:**

```
═══════════════════════════════════════════════════════════════
STEP 1: Calculate Balance Targets
═══════════════════════════════════════════════════════════════
  perWorkerTotalTasksMin = 24
  perWorkerTotalTasksMax = 25

═══════════════════════════════════════════════════════════════
STEP 2: Check for Violations
═══════════════════════════════════════════════════════════════
  Current state:
    w0-w7: now have 23-24 tasks each
    6 tasks still unassigned
  
  Global violations:
    Most workers at or near min(24) ✓
    underloadedWorkers = [subset with 23 tasks]
  
  Decision: Execute Step 4 (Assignment)

═══════════════════════════════════════════════════════════════
STEP 4: ASSIGNMENT GENERATION
═══════════════════════════════════════════════════════════════
  Assign final 6 tasks to workers with lowest count
  
  Result:
    - Final state: w0-w7 each have 24-25 tasks (perfectly balanced) ✅
```

**Summary:**
- Convergence: 5 generations (K = ceil(38/8) ≈ 5 generations)
- Each generation: Steps 1-2 always execute, Step 4 assigns tasks
- NO revocations (Step 3 never executed - no overload violations)
- Existing 156 tasks undisturbed, only 38 lost tasks reassigned incrementally

### Scale-Down Handling (Delayed Rebalance)

When workers **leave** the cluster, we use delayed rebalancing followed by incremental Assignment Generations.

#### Step 1: Delayed Rebalance Activation
```
Detect workers left cluster
  ↓
Start delayed rebalance timer (configured value, e.g., 30 seconds)
  ↓
During delay: No changes occur (allows workers to rejoin)
```

**Configuration:** `scheduled.rebalance.max.delay.ms` (30000ms from deployment config)

#### Step 2: After Delay Expires
```
Lost tasks from departed workers → marked as unassigned
  ↓
Recalculate targets for NEW worker count
  ↓
Proceed to Assignment Generation (NO revocations needed)
```

**Why No Revocations Needed:**
- Remaining workers keep their current tasks
- Only lost tasks need reassignment
- Current worker loads are typically < new globalMin

#### Step 3: Incremental Assignment Generations
- Each generation assigns 1 task to each underloaded worker
- Workers rejoin after each assignment
- Continue until all lost tasks redistributed and balance achieved

### Assignment Example: New Tasks

**Scenario:** Connector c1 increased from 111 to 120 tasks (9 new tasks)

**Generation N (Assignment):**
```
Current: w0-w9 each have 19-20 tasks total
Unassigned: 9 new c1 tasks
New targets: globalMax=20 (rounded from 203/10)
Per-consumer: c1max=12

Decision: 9 unassigned tasks exist, no overloaded workers
Action: Assign 1 c1 task to workers with lowest c1 count

Result:
  Revocations: (none)
  Assignments: 9 workers each get 1 new c1 task
  
Final: All workers 20-21 tasks, perfectly balanced
Convergence: 1 generation (all 9 tasks assigned to different workers)
```

## Complete Examples with Actual Task Counts

### Example 1: Scale-Up Scenario (10→12 workers)

#### Scenario
- **Before**: 10 workers with 194 tasks (perfectly balanced at 19-20 tasks per worker)
- **After**: 12 workers (2 new workers joined: w10, w11)
- **Total tasks**: 194

#### Initial State (10 workers)
```
w0-w9: each has 19-20 tasks (balanced for 10 workers)
NEW: w10, w11 have 0 tasks each
```

#### New Targets (12 workers)
```
perWorkerTotalTasksMin = floor(194/12) = 16
perWorkerTotalTasksMax = ceiling(194/12) = 17
perWorkerTotalTasksMaxLimit = 18

Per-consumer:
  c1: min=9, max=10 (was 11, 12)
  c2: min=2, max=3 (was 3, 4)
  c3-c5: min=1, max=2 (was 1, 2)
  c6-c9: min=0, max=1 (unchanged)
```

#### Rebalancing Process (Incremental Revocations + Assignments)

**Generation 1 (Revocation):**
```
Current: w0-w9 have 19-20 tasks (all > perWorkerTotalTasksMax=17), w10-w11 have 0
Decision: Workers w0-w9 are overloaded
Action: Revoke 1 task from each of w0-w9

Result:
  Revocations: w0→revoke 1 task, w1→revoke 1 task, ..., w9→revoke 1 task
  Total revoked: 10 tasks
  New state: w0-w9 have 18-19 tasks, w10-w11 have 0 tasks, 10 unassigned
```

**Generation 2 (Assignment):**
```
Current: 10 unassigned tasks, w10-w11 are underloaded
Decision: Assign to underloaded workers
Action: Assign 1 task to each of w10-w11, then to least loaded of w0-w9

Result:
  Assignments: w10→1 task, w11→1 task, and 8 more to w0-w7
  New state: w0-w7 have 19-20, w8-w9 have 18-19, w10-w11 have 1, all balanced
```

**Generation 3 (Revocation):**
```
Current: Some workers still > perWorkerTotalTasksMax=17
Decision: Continue revoking from overloaded workers
Action: Revoke 1 task from each overloaded worker

...continue for N more generations...
```

**Final State (after ~5-7 generations):**
```
w0-w11: Each has 16-17 tasks (perfectly balanced)
Per-consumer balanced: ✅ All within [perConsumerPerWorkerTaskCountMin, perConsumerPerWorkerTaskCountMax]
Globally balanced: ✅ All workers within [16, 17]
Convergence: ~5-7 generations (incremental revocations and assignments)
Disruption: Gradual - only ~3-4 tasks revoked per worker over multiple generations
```

---

### Example 2: Scale-Down Scenario (10→8 workers)

#### Initial State (10 workers, 194 tasks)
```
Workers: w0-w9
Each worker: 19-20 tasks (perfectly balanced)

w8: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) = 19 tasks
w9: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) = 19 tasks
```

#### Event: Workers Terminated
```
w8, w9 terminated → 38 tasks lost
Delayed rebalance: 30 seconds (waiting for rejoin)
```

#### After Delay Expires (8 workers remain)

**Active workers:**
```
w0-w7 (still running with existing tasks)
```

**Unassigned tasks:**
```
38 tasks from w8, w9:
  22 c1 tasks
  8 c2 tasks
  2 c3 tasks
  4 c4 tasks
  2 c5 tasks
```

**NEW Targets (8 workers, 194 tasks):**
```
Global:
  perWorkerTotalTasksMin = floor(194/8) = 24
  perWorkerTotalTasksMax = ceiling(194/8) = 25
  perWorkerTotalTasksMaxLimit = 26

Per-Consumer:
  c1: min=13, max=14 (was 11, 12)
  c2: min=4, max=5 (was 3, 4)
  c3-c5: min=1, max=2 (unchanged)
  c6-c9: min=0, max=1 (unchanged)
```

#### Rebalancing Process (No Revocations Needed)

**Check for Revocations:**
```
Any empty workers? NO (w0-w7 have 19-20 tasks)
Any overloaded workers? NO (all have 19-20, which is < perWorkerTotalTasksMaxLimit=26)
Result: Skip Revocation, go directly to Assignment
```

**Generation 1 (Assignment):**
```
Current: w0-w7 have 19-20 tasks each (all underloaded vs globalMin=24)
Unassigned: 38 tasks
Decision: Assign to underloaded workers
Action: Assign 1 task to each of 8 workers

Result:
  Assignments: w0→c1-task-X, w1→c1-task-Y, ..., w7→c2-task-Z
  New state: w0-w7 have 20-21 tasks, 30 unassigned remain
```

**Generation 2 (Assignment):**
```
Current: w0-w7 have 20-21 tasks (still underloaded)
Unassigned: 30 tasks
Action: Assign 1 task to each of 8 workers

Result:
  New state: w0-w7 have 21-22 tasks, 22 unassigned remain
```

**Generation 3-5 (Assignments):**
```
Continue assigning 1 task per worker per generation...
```

**Final State (Generation 5):**
```
Workers: w0-w7
Each worker: 24-25 tasks (perfectly balanced)

Per-consumer balance:
  c1: Each worker has 13-14 tasks ✅
  c2: Each worker has 4-5 tasks ✅
  c3-c5: Each worker has 1-2 tasks ✅
  c6-c9: Each worker has 0-1 tasks ✅

Global balance:
  All workers within [24, 25] ✅

Convergence: 5 generations (incremental assignments, ceil(38/8) = 5)
Disruption: None to existing 156 tasks, only 38 lost tasks reassigned incrementally
```

---

### Example 3: Task Count Increase (c1: 111→120 tasks)

#### Initial State (10 workers, 194 total tasks)
```
Workers: w0-w9
Each worker: 19-20 tasks total
c1 distribution: 11-12 c1 tasks per worker (111 total)

Perfectly balanced for 194 tasks:
  c1min=11, c1max=12
  perWorkerTotalTasksMin=19, perWorkerTotalTasksMax=20
```

#### Event: Task Count Increased
```
User action: Update connector-1 config
  tasks.max: 111 → 120

Connector generates 9 additional tasks
ConfigBackingStore writes new task configs
Rebalance triggered
```

#### New State
```
ConfigSnapshot: c1 now has 120 tasks
Unassigned: 9 new c1 tasks
Total tasks: 203 (194 + 9)
```

#### New Targets (10 workers, 203 tasks)
```
Global:
  perWorkerTotalTasksMin = floor(203/10) = 20
  perWorkerTotalTasksMax = ceiling(203/10) = 21
  perWorkerTotalTasksMaxLimit = 22

Per-Consumer c1:
  c1min = floor(120/10) = 12
  c1max = ceiling(120/10) = 12
```

#### Rebalancing Process

**Check for Revocations:**
```
Any overloaded workers? w0-w9 have 19-20 (all ≤ perWorkerTotalTasksMaxLimit=22)
Result: No revocations needed, go to Assignment
```

**Generation 1 (Assignment):**
```
Current: w0-w9 have 19-20 tasks total, 11-12 c1 tasks each
Unassigned: 9 new c1 tasks
Decision: Assign new c1 tasks to workers with lowest c1 count

Action: Assign 1 task to each worker with only 11 c1 tasks

Result:
  9 workers get 1 new c1 task each
  All workers now have 20-21 total tasks
  All workers now have 12 c1 tasks
  Perfectly balanced!
```

**Final State:**
```
Workers: w0-w9
Each worker: 20-21 tasks total ✅
Each worker: 12 c1 tasks ✅

Convergence: 1 generation (all 9 tasks assigned simultaneously to different workers)
Disruption: Zero - only new tasks assigned, no revocations
```

---

## Summary: Incremental Cooperative Rebalancing with Intelligent Forecasting

### Core Principle

**Forecast target state, then revoke/assign 1 task per consumer per worker at a time, re-sorting after each action.**

This creates a truly intelligent incremental rebalancing protocol that:
- Forecasts the target balanced state before taking action
- Checks balance before rebalancing (avoids unnecessary work)
- Revokes/assigns 1 task per consumer per worker (not just 1 task total)
- Re-sorts cluster state after each action
- Respects the cooperative protocol's constraint (revoke OR assign per generation, not both)
- Maintains continuous balance improvement every generation
- Prioritizes per-consumer balance (reaches perConsumerPerWorkerTaskCountMin before distributing extras)

### Comparison: Old vs Enhanced Approach

| Aspect | Old (Simple Incremental) | New (Intelligent Forecasting) |
|--------|------------------------|-------------------|
| **Balance Check** | Implicit | Explicit - checks before acting |
| **Forecasting** | None | Projects target state before revoking/assigning |
| **Revocation Unit** | 1 task per worker | 1 task **per consumer** per worker |
| **Assignment Unit** | 1 task per worker | 1 task **per consumer** per worker |
| **Sorting Frequency** | Once per generation | After **each** revocation/assignment |
| **Priority Strategy** | Simple round-robin | Consumer-first with severity ordering |
| **Convergence** | N generations | Optimized N (forecasted path) |
| **Per-Consumer Balance** | Eventually achieved | Prioritized in every action |
| **Disruption** | Minimal | Minimal + Intelligent |

### Benefits of Intelligent Forecasting Approach

1. **Faster Convergence**
   - Forecasting identifies exact actions needed
   - No wasted revocations or assignments
   - Optimal path to balanced state

2. **Superior Per-Consumer Fairness**
   - 1 task per consumer per worker ensures balanced growth/shrinkage
   - Prevents over-concentration on single consumer
   - Phase A ensures consumerNTaskCountMin reached before Phase B
   - **NEW**: `maxNumberOfWorkersWithPerConsumerTaskCountMax` ensures mathematically perfect distribution

3. **Continuous Re-sorting**
   - After each single task movement
   - Maintains global fairness throughout process
   - Always assigns to least-loaded eligible worker

4. **Intelligent Prioritization**
   - Revokes from most overloaded consumers first
   - Assigns to consumers furthest from perConsumerPerWorkerTaskCountMin first
   - Optimizes for both global and per-consumer balance simultaneously
   - **NEW**: Prevents over-distribution of max task counts

5. **Avoids Unnecessary Rebalancing**
   - Balance check at start of each generation
   - Only acts if truly needed
   - Saves cluster resources

6. **Predictable Forecasting**
   - Projects target state before acting
   - Clear visibility into how many generations needed
   - Easy to monitor and reason about
   - **NEW**: Enforces exact distribution constraints for perfect balance

### When to Use Each Generation Type

| Current State | Violation Check Result | Action | Generation Type |
|---------------|----------------------|--------|-----------------|
| All workers within [perWorkerTotalTasksMin, perWorkerTotalTasksMaxLimit]<br>All consumers within [perConsumerPerWorkerTaskCountMin, perConsumerPerWorkerTaskCountMax]<br>All consumers respect maxNumberOfWorkersWithPerConsumerTaskCountMax<br>No unassigned tasks | ✅ NO VIOLATIONS | No action | **No Rebalance** |
| Workers with totalTasks > perWorkerTotalTasksMaxLimit<br>OR consumers with count > perConsumerPerWorkerTaskCountMax<br>OR consumers with workersWithMaxCount > maxNumberOfWorkersWithPerConsumerTaskCountMax | ❌ OVERLOAD VIOLATIONS | Revoke ALL excess tasks from violated workers/consumers<br>Revoke excess max-count workers to meet constraint<br>Sort by violation severity<br>Return revocations only | **Revocation** |
| Workers with totalTasks < perWorkerTotalTasksMin<br>OR consumers with count < perConsumerPerWorkerTaskCountMin<br>OR unassigned tasks exist | ❌ UNDERLOAD VIOLATIONS | Phase A: Fill deficits to reach min targets<br>Phase B: Distribute remaining unassigned tasks<br>Respect maxNumberOfWorkersWithPerConsumerTaskCountMax during assignment<br>Return assignments only | **Assignment** |

### Algorithm Flow Per Generation

**CRITICAL**: Steps 1-2 execute EVERY generation. Steps 3-4 are mutually exclusive based on Step 2's violation detection.

```
START Generation N
  ↓
═══════════════════════════════════════════════════════════════
STEP 1 (ALWAYS EXECUTED): Calculate Targets
═══════════════════════════════════════════════════════════════
   Calculate for current worker count:
     - perWorkerTotalTasksMin = floor(totalTasks / numWorkers)
     - perWorkerTotalTasksMax = ceiling(totalTasks / numWorkers)
     - perWorkerTotalTasksMaxLimit = perWorkerTotalTasksMax + 1
     
     For each consumer:
       - perConsumerPerWorkerTaskCountMin = floor(consumerTasks / numWorkers)
       - perConsumerPerWorkerTaskCountMax = ceiling(consumerTasks / numWorkers)
  ↓
═══════════════════════════════════════════════════════════════
STEP 2 (ALWAYS EXECUTED): Check for Violations
═══════════════════════════════════════════════════════════════
   A. Detect Global Violations:
      overloadedWorkers = workers with totalTasks > perWorkerTotalTasksMaxLimit
      underloadedWorkers = workers with totalTasks < perWorkerTotalTasksMin
   
   B. Detect Per-Consumer Violations:
      consumerOverloads = (worker, consumer) pairs where
                          worker.consumerTaskCount > perConsumerPerWorkerTaskCountMax
      consumerUnderloads = (worker, consumer) pairs where
                           worker.consumerTaskCount < perConsumerPerWorkerTaskCountMin
   
   C. Identify Unassigned Tasks:
      unassignedTasks = tasks not yet assigned to any worker
  ↓
═══════════════════════════════════════════════════════════════
DECISION POINT: Choose Action Based on Violations
═══════════════════════════════════════════════════════════════
   
   IF overloadedWorkers NOT empty OR consumerOverloads NOT empty:
     ↓
     ┌────────────────────────────────────────────────────────┐
     │ STEP 3: REVOCATION GENERATION (Overload violations)    │
     └────────────────────────────────────────────────────────┘
     
     Action: Revoke excess tasks from violated workers/consumers
     
     Step 3a: Revoke from per-consumer overloads
       - For each (worker, consumer) with violations
       - Calculate excessCount = actual - max
       - Revoke ALL excess tasks
       - Update worker state after each revocation
     
     Step 3b: Revoke from globally overloaded workers
       - For each worker with totalTasks > maxLimit
       - Select consumer with most tasks on worker
       - Revoke tasks until totalTasks ≤ maxLimit
       - Update worker state after each revocation
     
     Return: ClusterAssignment with:
       - newlyRevokedTasks: all revocations
       - newlyAssignedTasks: EMPTY
       - allAssignedTasks: current minus revocations
     
     Workers will:
       - Stop revoked tasks
       - Rejoin cluster
       - Next generation re-runs Steps 1-2
     ↓
     SKIP Step 4 (no assignments in revocation generation)
     ↓
     
   ELSE IF underloadedWorkers NOT empty OR consumerUnderloads NOT empty OR unassignedTasks NOT empty:
     ↓
     ┌────────────────────────────────────────────────────────┐
     │ STEP 4: ASSIGNMENT GENERATION (No overload violations)  │
     └────────────────────────────────────────────────────────┘
     
     Precondition Check:
       ✅ overloadedWorkers IS empty
       ✅ consumerOverloads IS empty
       (Ensures we only assign, never revoke)
     
     Action: Fill deficits and assign unassigned tasks
     
     Phase A: Fill to minimum targets
       - Fill per-consumer deficits first
         * For each (worker, consumer) in consumerUnderloads
         * Calculate deficitCount = min - actual
         * Assign deficitCount tasks from unassigned pool
         * Update worker state after each assignment
       
       - Fill globally underloaded workers
         * For each worker in underloadedWorkers
         * Assign tasks until totalTasks ≥ min
         * Select consumer with fewest tasks on worker
         * Update worker state after each assignment
     
     Phase B: Distribute remaining unassigned tasks
       - While unassigned tasks remain
       - Assign to worker with lowest totalTasks (≤ max)
       - Select consumer with fewest tasks on worker
       - Update worker state after each assignment
     
     Return: ClusterAssignment with:
       - newlyAssignedTasks: all assignments
       - newlyRevokedTasks: EMPTY
       - allAssignedTasks: current plus assignments
     
     Workers will:
       - Start newly assigned tasks
       - Continue running existing tasks
       - Next generation re-runs Steps 1-2
     ↓
     SKIP Step 3 (no revocations in assignment generation)
     ↓
     
   ELSE:
     ↓
     ┌────────────────────────────────────────────────────────┐
     │ NO ACTION: Cluster is Balanced                          │
     └────────────────────────────────────────────────────────┘
     
     All violations satisfied:
       ✅ overloadedWorkers IS empty
       ✅ consumerOverloads IS empty
       ✅ underloadedWorkers IS empty
       ✅ consumerUnderloads IS empty
       ✅ unassignedTasks IS empty
     
     Return: ClusterAssignment with:
       - newlyAssignedTasks: EMPTY
       - newlyRevokedTasks: EMPTY
       - allAssignedTasks: unchanged
     
     Workers continue with current assignments
     ↓
     SKIP Steps 3 and 4 (no changes needed)
  ↓
═══════════════════════════════════════════════════════════════
GENERATION COMPLETE
═══════════════════════════════════════════════════════════════
  Workers apply changes (EITHER revocations OR assignments OR nothing)
  Workers rejoin cluster for next generation
  Next generation repeats from Step 1

═══════════════════════════════════════════════════════════════
KEY PRINCIPLES
═══════════════════════════════════════════════════════════════
1. Steps 1-2 ALWAYS execute every generation
2. Step 3 (Revocation) and Step 4 (Assignment) are MUTUALLY EXCLUSIVE
3. Never revoke AND assign in the same generation
4. Step 3 takes precedence: overload violations are fixed before assignments
5. Workers rejoin after each generation, triggering next generation's Steps 1-2
```

### Configuration Recommendations

Based on this incremental approach:

```properties
# Reduced rebalance timeout - generations are fast with minimal work
CONNECT_REBALANCE_TIMEOUT_MS=120000  # 2 minutes

# Faster session timeout - detect failures quickly
CONNECT_SESSION_TIMEOUT_MS=30000  # 30 seconds

# Shorter delayed rebalance - start rebalancing sooner
CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS=30000  # 30 seconds

# Longer task shutdown - allow clean offset flushing
CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS=100000  # 100 seconds
```

These settings from the deployment config are optimized for:
- Fast failure detection (30s session timeout)
- Quick rebalance initiation (30s delay)
- Sufficient time for each incremental step (120s rebalance timeout)
- Clean task shutdown (100s graceful timeout)

---

## Conclusion

The violation-based cooperative rebalancing approach transforms Kafka Connect's distributed task assignment into a precise, violation-driven process. By checking for specific violations first (overload vs. underload) and then taking targeted action, we achieve:

- **Violation-Driven Decision Making**: Check THREE violation types (global, per-consumer, and distribution) before deciding action
- **Targeted Revocations**: Revoke ONLY when overload violations detected (workers > maxLimit or consumers > max or distribution constraint violated)
- **Targeted Assignments**: Assign ONLY when underload violations detected or unassigned tasks exist
- **No Wasted Generations**: Take action only when violations exist, no action when balanced
- **Clear Separation**: Revocation handles overload, Assignment handles underload - never mixed
- **Cooperative Protocol Compliance**: Each generation performs ONLY revocations OR assignments, never both
- **Efficient Deficit Filling**: Directly calculate and fill deficits instead of 1 task per iteration
- **Fair Distribution**: Remaining tasks distributed evenly across least-loaded workers
- **Mathematical Precision**: `maxNumberOfWorkersWithPerConsumerTaskCountMax` ensures exactly the right number of workers have maximum task count
- **Better cloud-native fit**: Handles spot instance volatility gracefully with minimal per-generation disruption
- **Lower operational risk**: Each generation moves exactly one task per consumer per worker
- **Clearer reasoning**: Explicit balance checks and forecasting make behavior predictable
- **Protocol alignment**: Natural fit with cooperative rebalancing philosophy

### Key Innovation: Forecasting + Per-Consumer Granularity + Distribution Constraint

The combination of forecasting, per-consumer task movement, and distribution constraints is crucial:

1. **Forecasting** tells us WHERE we need to be (target state)
2. **Per-consumer granularity** ensures we get there FAIRLY (1 task per consumer per worker)
3. **Re-sorting** maintains BALANCE throughout (after each action)
4. **Balance checks** avoid UNNECESSARY work (only act when needed)
5. **Distribution constraint** (`maxNumberOfWorkersWithPerConsumerTaskCountMax`) ensures MATHEMATICAL PRECISION (exactly the right number of workers have max count)

### Example: Why Per-Consumer Matters

**Without per-consumer approach:**
```
Generation 1: Revoke 1 task total from w0
  Could revoke c1 task, leaving c2 still overloaded
  Next generation must revoke c2 task
  Result: 2 generations, serial balancing
```

**With per-consumer approach:**
```
Generation 1: Revoke 1 c1 task AND 1 c2 task from w0
  Addresses both consumers simultaneously
  Result: 1 generation, parallel balancing
```

This approach is particularly valuable in large-scale, multi-tenant environments where:
- Hundreds of connectors with vastly different task counts run simultaneously
- Workers frequently join/leave due to autoscaling or spot terminations
- Continuous operation is critical (downtime is costly)
- Balance must be maintained across BOTH global AND per-connector dimensions simultaneously
- Forecasting ensures optimal convergence path
- Per-consumer granularity ensures fairness is never sacrificed

The intelligent forecasting approach ensures that Kafka Connect clusters remain healthy, balanced, and resilient even in the face of constant change, while optimizing the path to perfect balance.


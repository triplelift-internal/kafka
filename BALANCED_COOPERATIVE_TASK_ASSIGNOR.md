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

This algorithm provides a fast-converging, balanced task distribution strategy for Kafka Connect in distributed mode using the cooperative rebalancing protocol. It is specifically designed for large-scale, multi-tenant, spot-instance environments where fast convergence and per-consumer fairness are critical.

**Design Goals:**
1. **Fast convergence**: Maximum 2 generations to reach perfect balance (vs 3+ with incremental approaches)
2. **Per-consumer fairness**: Each consumer (connector) must be balanced across all workers
3. **Global balance**: Total task load balanced across all workers
4. **Minimal disruption**: Preserves existing assignments when possible (scale-down, task additions)
5. **Spot-instance optimized**: Handles frequent worker churn efficiently

---

## Example Configuration

Throughout this document, we use the following example configuration:

| Parameter | Value |
|-----------|-------|
| **Total Workers** | 10 |
| **Total Consumers (Connectors)** | 9 |
| **Total Tasks** | 194 |

### Task Distribution by Consumer

| Consumer | Task Count | Tasks/Worker | consumerN_task_count_min | consumerN_task_count_max |
|----------|------------|--------------|--------------------------|--------------------------|
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
- `globalMin` = floor(194/10) = **19** tasks per worker
- `globalMax` = ceiling(194/10) = **20** tasks per worker
- `globalMaxLimit` = globalMax + 1 = **21** tasks per worker (tolerance threshold)

**Per-Consumer Balance Targets:**
- `consumerN_task_count_min` = floor(consumer_tasks / total_workers) - Minimum tasks each worker should have from consumer N
- `consumerN_task_count_max` = ceiling(consumer_tasks / total_workers) - Maximum tasks each worker can have from consumer N

**Objective:** Achieve balanced distribution where:
- Each worker has [globalMin, globalMax] total tasks
- For each consumer N, each worker has [consumerN_task_count_min, consumerN_task_count_max] tasks from that consumer

---
---

## Algorithm Structure

The algorithm operates in two possible rounds:

1. **Round 1 (Revocations)**: Full revocation of all tasks - triggered only on scale-up or severe imbalance
2. **Round 2 (Complete Assignment)**: Assigns all unassigned tasks with perfect balance - always executes when tasks need redistribution

### Maximum Convergence Time
- **Scale-up scenarios**: 2 generations (Round 1 → Round 2)
- **Scale-down scenarios**: 1 generation (Round 2 only)
- **Task/connector changes**: 1-2 generations depending on whether Round 1 is triggered

---

## Round 1: Revocation Phase

**Purpose:** Full revocation of all tasks to achieve optimal redistribution across ALL workers.

**Operation:** Revoke only (no assignments) - all workers end with zero tasks.

### Trigger Criteria

Round 1 is triggered if **ANY** of these conditions are met:

#### 1. Scale-Up Detection (Empty Workers)
```
IF any worker has 0 assigned tasks
  → TRIGGER Round 1
```

**Scenarios:**
- New workers joined the cluster (10→12 workers)
- New deployment with fresh workers
- New connector added with many tasks creating empty workers

**Why needed:** Without full revocation, new workers stay empty while existing workers remain loaded.

**Example:**
```
Before: w0-w9 each have ~19 tasks, w10-w11 have 0 tasks
After Round 1: All workers (w0-w11) have 0 tasks
After Round 2: All workers (w0-w11) have ~16 tasks each
```

#### 2. Severe Imbalance Detection
```
IF any worker has > globalMaxLimit tasks
  → TRIGGER Round 1
```

**Scenarios:**
- Extreme load imbalance due to previous failures
- Manual task assignment errors

**Example:**
```
globalMaxLimit = 21
Worker w0 has 30 tasks → TRIGGER Round 1
```

### What Does NOT Trigger Round 1

Round 1 is **NOT** triggered for:
- ❌ Worker scale-down (departed workers)
- ❌ Task count increases (new tasks added to existing connector)
- ❌ Task count decreases (tasks removed from existing connector)
- ❌ Connector removal (deleted tasks filtered out automatically)

These scenarios proceed **directly to Round 2** for incremental rebalancing.

### Round 1 Output

After Round 1 completes:
- All tasks revoked from all workers
- All tasks marked as "unassigned"
- Ready for redistribution in Round 2

---

## Scale-Down Handling (Special Case)

When workers **leave** the cluster, we do **NOT** trigger Round 1. Instead, we use a gentler incremental approach.

### Process Flow

#### Step 1: Delayed Rebalance Activation
```
Detect workers left cluster
  ↓
Start delayed rebalance timer (e.g., 5 minutes)
  ↓
During delay: No changes occur (allows workers to rejoin)
```

**Configuration:** `scheduled.rebalance.max.delay.ms` (default: 5 minutes)

#### Step 2: After Delay Expires
```
Lost tasks from departed workers → marked as unassigned
  ↓
Recalculate targets for NEW worker count
  ↓
Proceed directly to Round 2 (NO revocations)
```

**New Targets Calculation:**
- New `globalMin`, `globalMax`, `globalMaxLimit` based on remaining workers
- New `consumerN_task_count_min`, `consumerN_task_count_max` for each consumer based on remaining workers

#### Step 3: Round 2 Incremental Assignment
- **Phase A**: Fill remaining workers to NEW consumerN_task_count_min values
- **Phase B**: Distribute lost tasks using 1-task-at-a-time logic
- **Preservation**: Existing tasks on remaining workers stay assigned

### Scale-Down Example

**Scenario:** 10 workers → 8 workers (w8, w9 terminated)

#### Initial State (10 workers)
```
w0-w9: each has 19-20 tasks (194 total)
w8: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) + 0(c6-c9) = 19 tasks
w9: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) + 0(c6-c9) = 19 tasks
```

#### Event: w8, w9 Terminated
```
Lost tasks: 38 tasks
Delayed rebalance: 5 minutes (no changes during this time)
```

#### After Delay Expires (8 workers remain)
```
Active workers: w0-w7
Unassigned tasks: 38 tasks from w8, w9

NEW Targets (8 workers):
  globalMin = floor(194/8) = 24
  globalMax = ceiling(194/8) = 25
  c1: min=13, max=14 (was 11, 12 for 10 workers)
  c2: min=4, max=5 (was 3, 4)
  c3-c9: unchanged
```

#### Round 1 Check
```
Any empty workers? NO (w0-w7 still have tasks)
Any overloaded workers? NO
Result: SKIP Round 1
```

#### Round 2 Execution
**Phase A - Fill to NEW consumerN_task_count_min:**
```
w0 current: 11 c1 tasks, NEW target: 13 c1 tasks → Assign 2 more c1 tasks
w0 current: 3 c2 tasks, NEW target: 4 c2 tasks → Assign 1 more c2 task
... (repeat for w1-w7)
```

**Phase B - Distribute Remaining:**
```
Sort workers by total load (ascending)
Assign remaining tasks 1-at-a-time to least loaded workers
```

#### Final State (8 workers)
```
w0-w7: each has 24-25 tasks (perfectly balanced)
Per-consumer: All within [consumerN_task_count_min, consumerN_task_count_max] for 8 workers
Global: All within [24, 25]
Convergence: 1 generation (no revocation needed)
Disruption: Minimal (only 38 lost tasks reassigned)
```

### Why Different Handling?

| Scenario | Approach | Reason |
|----------|----------|--------|
| **Scale-Up** | Full revocation (Round 1) | Must redistribute existing work to include new workers |
| **Scale-Down** | Incremental assignment (Round 2 only) | Remaining workers naturally absorb lost tasks without disrupting existing assignments |

---
---

## Round 2: Complete Assignment Phase

**Purpose:** Assign all unassigned tasks with perfect per-consumer and global balance.

**Operation:** Assignment only (no revocations) - consists of two sequential phases within a single generation.

### Input: ConfigSnapshot

Round 2 always operates on the current state from `ConfigBackingStore`:

```java
configSnapshot.connectors()           // Set of connectors that SHOULD exist
configSnapshot.tasks(connector)       // Set of tasks that SHOULD be running for each connector
memberAssignments                     // Current worker assignments (what IS running)
```

**Automatic Filtering:**
- Any task in `memberAssignments` but NOT in `configSnapshot` → Automatically filtered out (deleted)
- Any task in `configSnapshot` but NOT in `memberAssignments` → Marked as "unassigned"

### Unassigned Tasks Sources

Round 2 handles ALL unassigned tasks, regardless of source:

| Source | Description | Example |
|--------|-------------|---------|
| **Round 1 Revocation** | All tasks revoked in Round 1 | Scale-up: 194 tasks unassigned |
| **Worker Scale-Down** | Tasks from departed workers | 10→8 workers: 38 tasks lost |
| **New Connector** | All tasks from new connector | connector-10 added: 50 new tasks |
| **Task Count Increase** | New tasks from existing connector | c1: 111→120 tasks: 9 new tasks |
| **Connector Removal** | Remaining tasks after deletion | After c9 removed: 193 tasks remain |
| **Task Count Decrease** | Remaining tasks after deletion | c1: 111→100 tasks: 11 tasks removed |

**Key Principle:** The same algorithm applies regardless of WHY tasks are unassigned.

---

### Phase A: Fill to consumerN_task_count_min (Minimum Guarantee)

**Goal:** Ensure every worker gets **at least** consumerN_task_count_min tasks from each consumer before distributing extras.

**Strategy:** 1-task-at-a-time assignment with re-sorting to maintain perfect balance throughout.

#### Algorithm Steps

**Step 1: Identify Unassigned Tasks**
```
For each consumer in configSnapshot.connectors():
  unassignedTasks[consumer] = configSnapshot.tasks(consumer) - memberAssignments.tasks(consumer)
```

**Examples by scenario:**
- Scale-up: All 194 tasks (after Round 1 revocation)
- Scale-down (10→8): 38 tasks from departed w8, w9
- New connector: All 50 tasks from connector-10
- Task increase (c1: 111→120): 9 new c1 tasks
- Connector removed: Remaining 193 tasks (c9's 1 task filtered out)

**Step 2: Calculate Current Targets**
```
globalMin = floor(totalTasks / currentWorkerCount)
globalMax = ceiling(totalTasks / currentWorkerCount)
globalMaxLimit = globalMax + 1

For each consumer:
  consumerN_task_count_min = floor(consumerTasks / currentWorkerCount)
  consumerN_task_count_max = ceiling(consumerTasks / currentWorkerCount)
```

**Critical:** Use CURRENT worker count, not previous count!

**Example after scale-down (10→8 workers):**
```
OLD targets (10 workers): c1min=11, c1max=12
NEW targets (8 workers):  c1min=13, c1max=14
```

**Step 3: Sort Consumers by consumerN_task_count_max (Descending)**
```
Sort order: Consumers with highest consumerN_task_count_max first
Example: c1(consumerN_task_count_max=12), c2(4), c3(2), c4(2), c5(2), c6(1), c7(1), c8(1), c9(1)
```

**Step 4: Fill to consumerN_task_count_min Per Consumer**
```java
for each consumer in sorted order:
  if unassignedTasks[consumer].isEmpty():
    skip to next consumer
    
  if consumerN_task_count_min == 0 for this consumer:
    skip to next consumer  // Handle in Phase B
    
  while workers exist below consumerN_task_count_min for this consumer:
    sort workers by totalTaskLoad (ascending - least loaded first)
    
    for each worker:
      if worker.tasksFromConsumer < consumerN_task_count_min AND unassignedTasks remain:
        assign EXACTLY 1 task to this worker
        remove task from unassignedTasks
        add task to worker's assignment
        BREAK and re-sort  // Critical for balance!
        
    if no progress made:
      break  // All workers at consumerN_task_count_min OR no tasks left
```

**Re-sorting is Critical:**
```
Why re-sort after each task?
- Maintains global balance while filling per-consumer targets
- Ensures we always assign to least-loaded worker
- Prevents any worker from getting too far ahead

Example:
w0: 17 tasks, w1: 18 tasks, w2: 19 tasks
Assign 1 task to w0 (least loaded)
New state: w0: 18 tasks, w1: 18 tasks, w2: 19 tasks
Re-sort: w0, w1 tied for least loaded
Next task goes to w0 or w1
```

#### Phase A Output

After Phase A completes:
- ✅ Every worker has **at least** consumerN_task_count_min tasks from each consumer (where consumerN_task_count_min > 0)
- ✅ Global balance maintained throughout (via re-sorting)
- ✅ Remaining unassigned tasks ready for Phase B

#### Phase A Example: Scale-Down (10→8 workers)

**Initial State:**
```
w0 has: 11 c1 tasks (OLD c1min was 11)
NEW c1min = 13 (for 8 workers)
Gap: Need 2 more c1 tasks
```

**Phase A Execution:**
```
Consumer c1:
  w0: 11 c1 tasks → Assign 1 → 12 c1 tasks (re-sort)
  w1: 11 c1 tasks → Assign 1 → 12 c1 tasks (re-sort)
  ... continue until all workers at c1min=13

Consumer c2:
  Similar process to reach c2min=4
  
... continue for all consumers with consumerN_task_count_min > 0
```

**After Phase A:**
```
w0-w7: Each worker closer to 24 tasks
Per-consumer: All workers at or near consumerN_task_count_min for each consumer
```
```

**After Phase A:**
```
w0-w7: Each worker closer to 24 tasks
Per-consumer: All workers at or near consumerN_task_count_min for each consumer
```

---

### Phase B: Distribute Remaining Tasks

**Goal:** Distribute all remaining unassigned tasks while respecting consumerN_task_count_max and globalMaxLimit.

**Strategy:** 1-task-at-a-time assignment to least-loaded eligible workers.

#### Algorithm Steps

**Step 1: Check for Remaining Tasks**
```
if unassignedTasks is empty:
  return complete assignments  // Phase B done!
```

**Step 2: Sort Consumers by Remaining Task Count (Descending)**
```
Sort consumers by unassignedTasks[consumer].size(), largest first
Priority to consumers with most remaining tasks
```

**Step 3: Distribute Per Consumer**
```java
for each consumer in sorted order:
  remainingTasks = unassignedTasks[consumer]
  isSmallConsumer = (totalTasksForConsumer <= numWorkers)
  
  while remainingTasks not empty:
    sort workers by totalTaskLoad (ascending - least loaded first)
    
    find first eligible worker where:
      ✓ worker.totalTasks < globalMaxLimit
      ✓ worker.tasksFromConsumer < consumerN_task_count_max
      ✓ if isSmallConsumer: worker.tasksFromConsumer < 1  // Max 1 task per worker
      
    if no eligible worker found:
      if isSmallConsumer:
        log.warning("All workers have 1 task from small consumer")  // Expected
      else:
        log.error("No eligible worker found")  // Should not happen!
      break to next consumer
      
    assign EXACTLY 1 task to eligible worker
    remove from remainingTasks
    add to worker's assignment
    continue (re-sort for next iteration)
```

**Step 4: Return Complete Cluster Assignment**
```
All tasks assigned
Perfect per-consumer and global balance achieved
```

#### Special Handling: Small Consumers

**Definition:** Consumers with tasks <= workers (e.g., c6-c9 in our example)

**Behavior:**
- **Phase A**: Skipped (consumerN_task_count_min = 0)
- **Phase B**: Max 1 task per worker enforced

**Why?** Ensures fair distribution when fewer tasks than workers exist.

**Example:**
```
c9 has 1 task, 10 workers
Without limit: 1 worker gets the task, 9 workers get nothing ✓
With our limit: 1 worker gets 1 task, enforced by Phase B ✓
```

#### Phase B Example: Scale-Down (10→8 workers)

**After Phase A:**
```
w0-w7: Each has ~22-23 tasks
Unassigned: Some tasks remaining from 38 lost tasks
```

**Phase B Execution:**
```
Sort consumers by remaining unassigned count
For each consumer with remaining tasks:
  Sort workers: [w0: 22 tasks, w1: 22 tasks, w2: 23 tasks, ...]
  
  Assign 1 task to w0 (least loaded)
  New state: [w0: 23 tasks, w1: 22 tasks, w2: 23 tasks, ...]
  
  Re-sort: [w1: 22 tasks, w3: 22 tasks, w0: 23 tasks, ...]
  Assign 1 task to w1
  
  Continue until all remaining tasks assigned
```

**Final State:**
```
w0-w7: Each has 24-25 tasks (perfectly balanced)
Per-consumer: All within [consumerN_task_count_min, consumerN_task_count_max]
Global: All within [globalMin, globalMax]
```

---

---

## Complete Examples with Actual Task Counts

### Example 1: Scale-Up Scenario (0→10 workers or 10→12 workers)

#### Scenario
- **Before**: 0 workers (or 10 workers with 194 tasks)
- **After**: 10 workers (or 12 workers)
- **Total tasks**: 194

#### Round 1: Full Revocation
```
Trigger: Empty workers detected (new workers have 0 tasks)
Action: Revoke all 194 tasks from all workers
Result: All tasks unassigned, ready for Round 2
```

#### Round 2 Phase A: Fill to consumerN_task_count_min

**Targets for 10 workers:**
- c1: min=11, max=12
- c2: min=3, max=4
- c3: min=1, max=2
- c4: min=1, max=2
- c5: min=1, max=2
- c6-c9: min=0, max=1

**Assignment:**
```
Phase A assigns: 110(c1) + 30(c2) + 10(c3) + 10(c4) + 10(c5) = 170 tasks

Per worker after Phase A:
  11(c1) + 3(c2) + 1(c3) + 1(c4) + 1(c5) = 17 tasks each
```

#### Round 2 Phase B: Distribute Remaining

**Remaining unassigned:**
```
c1: 1 task
c2: 4 tasks
c3: 4 tasks
c4: 2 tasks
c5: 1 task
c6: 6 tasks
c7: 4 tasks
c8: 2 tasks
c9: 1 task
Total: 24 tasks
```

**Assignment:**
```
Distribute 1 task at a time to least-loaded workers
Respecting consumerN_task_count_max and globalMaxLimit constraints

Final distribution:
  4 workers @ 20 tasks
  6 workers @ 19 tasks
```

#### Final State
```
Per-consumer balanced: ✅ All consumers within [consumerN_task_count_min, consumerN_task_count_max]
Globally balanced: ✅ All workers within [19, 20]
Convergence: 2 generations (Round 1 → Round 2)
```

---

### Example 2: Scale-Down Scenario (10→8 workers)

#### Initial State (10 workers, 194 tasks)
```
Workers: w0, w1, w2, w3, w4, w5, w6, w7, w8, w9
Each worker: 19-20 tasks (perfectly balanced)

w8 tasks: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) + 0(c6) + 0(c7) + 0(c8) + 0(c9) = 19 tasks
w9 tasks: 11(c1) + 4(c2) + 1(c3) + 2(c4) + 1(c5) + 0(c6) + 0(c7) + 0(c8) + 0(c9) = 19 tasks
```

#### Event: Workers Terminated
```
w8 terminated → 19 tasks lost
w9 terminated → 19 tasks lost
Total lost: 38 tasks
```

#### Delayed Rebalance (5 minutes)
```
Time 0:00 - Workers w8, w9 left detected
Time 0:00 - Delayed rebalance timer started (5 minutes)
Time 0:00 to 4:59 - No changes occur (waiting for workers to rejoin)
Time 5:00 - Delayed rebalance expires
```

#### After Delay Expires (8 workers remain)

**Active workers:**
```
w0-w7 (still running with their existing tasks)
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
  globalMin = floor(194/8) = 24
  globalMax = ceiling(194/8) = 25
  globalMaxLimit = 26

Per-Consumer:
  c1: min=13, max=14 (was 11, 12)
  c2: min=4, max=5 (was 3, 4)
  c3: min=1, max=2 (unchanged)
  c4: min=1, max=2 (unchanged)
  c5: min=1, max=2 (unchanged)
  c6-c9: min=0, max=1 (unchanged)
```

#### Round 1 Check
```
Any empty workers? NO (w0-w7 still have 19-20 tasks each)
Any overloaded workers? NO (all under limit)
Result: SKIP Round 1 ✅
```

#### Round 2 Phase A: Fill to NEW consumerN_task_count_min

**Current state vs NEW targets:**
```
w0 current: 11 c1 tasks, NEW c1min=13 → Need 2 more c1 tasks
w0 current: 3 c2 tasks, NEW c2min=4 → Need 1 more c2 task
w0 current: 1 c3 task, NEW c3min=1 → Already at minimum ✓
... (similar for w1-w7)
```

**Assignment process:**
```
For consumer c1:
  Sort workers by total load: [w0: 19, w1: 19, w2: 20, ...]
  Assign 1 c1 task to w0 → w0: 20 tasks
  Re-sort: [w1: 19, w3: 19, w0: 20, w2: 20, ...]
  Assign 1 c1 task to w1 → w1: 20 tasks
  Continue until all workers have c1min=13

For consumer c2:
  Similar process to reach c2min=4

After Phase A:
  w0-w7: Each worker now has 22-23 tasks
```

#### Round 2 Phase B: Distribute Remaining

**Remaining unassigned:** Few tasks left after Phase A

**Assignment:**
```
Sort workers by total load
Assign 1 task at a time to least-loaded workers
Continue until all 38 tasks distributed
```

#### Final State (8 workers, 194 tasks)
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

Convergence: 1 generation (Round 2 only, no revocation)
Disruption: Minimal (only 38 lost tasks reassigned, 156 existing tasks preserved)
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
  globalMin=19, globalMax=20
```

#### Event: Task Count Increased
```
User action: Update connector-1 config
  tasks.max: 110 → 120

Connector action: Generate 9 additional task configs
  Tasks: connector-1-0 through connector-1-119

ConfigBackingStore: Write new task configs

Trigger: onTaskConfigUpdate() fires
  needsReconfigRebalance = true
  Rebalance triggered
```

#### During Rebalance

**ConfigSnapshot changes:**
```
configSnapshot.tasks("connector-1") now returns 120 tasks (was 111)
configuredTasks now has 203 total tasks (was 194)
```

**Current assignments:**
```
memberAssignments shows 194 tasks assigned
  111 c1 tasks distributed across workers
  83 other tasks distributed
```

**Unassigned tasks:**
```
9 new c1 tasks (connector-1-111 through connector-1-119)
```

#### Recalculate Targets (10 workers, 203 total tasks)

**NEW Targets:**
```
Global:
  globalMin = floor(203/10) = 20
  globalMax = ceiling(203/10) = 21
  globalMaxLimit = 22

c1 (120 tasks):
  c1min = floor(120/10) = 12 (was 11)
  c1max = ceiling(120/10) = 12 (was 12)

Other consumers: Unchanged
```

#### Round 1 Check
```
Any empty workers? NO (all workers have 19-20 tasks)
Any overloaded workers? NO (all under limit)
Result: SKIP Round 1 ✅
```

#### Round 2 Phase A: Fill to NEW consumerN_task_count_min

**Current state vs NEW c1min:**
```
c1min changed: 11 → 12

Workers with 11 c1 tasks: Need 1 more c1 task
```
```
c1min changed: 11 → 12

Workers with 11 c1 tasks: Need 1 more c1 task
Workers with 12 c1 tasks: Already at new c1min ✓
```

**Assignment:**
```
Sort workers by total load: [w0: 19 (11 c1), w1: 20 (12 c1), w2: 19 (11 c1), ...]

Assign 1 c1 task to w0 → w0: 20 tasks (12 c1)
Re-sort: [w2: 19 (11 c1), w4: 19 (11 c1), w0: 20 (12 c1), ...]
Assign 1 c1 task to w2 → w2: 20 tasks (12 c1)
Continue for all workers below c1min

After Phase A:
  All workers have 12 c1 tasks (c1min reached)
  Some tasks may remain if not all workers needed filling
```

#### Round 2 Phase B: Distribute Remaining (if any)
```
If any c1 tasks remain unassigned:
  Sort workers by total load
  Assign 1 task at a time to least-loaded workers
  Respecting c1max=12 and globalMaxLimit=22
```

#### Final State (10 workers, 203 total tasks)
```
Workers: w0-w9
Each worker: 20-21 tasks total (perfectly balanced)

c1 distribution:
  Each worker: 12 c1 tasks (120 total) ✅

Global balance:
  All workers within [20, 21] ✅

Convergence: 1 generation (Round 2 only, incremental)
Disruption: Minimal (only 9 new c1 tasks assigned, 111 existing c1 tasks unchanged)
```

---

---

## Convergence Guarantees

### Maximum Generations to Converge

| Scenario | Generations | Path |
|----------|-------------|------|
| Scale-up (empty workers) | **2** | Round 1 (Revoke) → Round 2 (Assign) |
| Scale-down (workers left) | **1** | Round 2 (Incremental Assign) |
| Task count increase | **1** | Round 2 (Incremental Assign) |
| New connector added | **1-2** | If creates empty workers: R1+R2, else R2 only |
| Connector/task removal | **1** | Round 2 (Rebalance remaining) |

**Comparison:** This algorithm converges in maximum 2 generations, compared to 3+ generations with traditional incremental approaches.

### Balance Constraints Enforced

#### Per-Consumer Balance
```
For each consumer N and each worker W:
  consumerN_task_count_min ≤ worker.tasksFromConsumer(N) ≤ consumerN_task_count_max

where:
  consumerN_task_count_min = floor(consumerN_totalTasks / totalWorkers)
  consumerN_task_count_max = ceiling(consumerN_totalTasks / totalWorkers)
```

**Example (c1 with 111 tasks, 10 workers):**
```
c1min = floor(111/10) = 11
c1max = ceiling(111/10) = 12
Result: Each worker has 11 or 12 c1 tasks ✅
```

#### Global Balance
```
For each worker W:
  globalMin ≤ worker.totalTasks ≤ globalMax

where:
  globalMin = floor(totalTasks / totalWorkers)
  globalMax = ceiling(totalTasks / totalWorkers)
```

**Example (194 tasks, 10 workers):**
```
globalMin = floor(194/10) = 19
globalMax = ceiling(194/10) = 20
Result: Each worker has 19 or 20 total tasks ✅
```

#### Small Consumer Fairness
```
For consumers with (totalTasks ≤ totalWorkers):
  Each worker gets at most 1 task from that consumer
```

**Example (c9 with 1 task, 10 workers):**
```
1 worker gets 1 c9 task
9 workers get 0 c9 tasks
Result: Fair distribution achieved ✅
```

---

## ClusterAssignment Tracking

The algorithm maintains consistency through the `ClusterAssignment` object across all rounds:

### Round 1 Tracking
```java
ClusterAssignment {
  revocations: Map<Worker, List<Task>>  // All tasks from all workers
  assignments: Map<Worker, List<Task>>  // Empty (no assignments in Round 1)
}
```

**Example:**
```
Round 1 revokes:
  w0: [c1-0, c1-1, c2-0, ...] → 19 tasks revoked
  w1: [c1-2, c1-3, c2-1, ...] → 20 tasks revoked
  ... (all workers)
```

### Round 2 Tracking
```java
ClusterAssignment {
  revocations: Map<Worker, List<Task>>  // Empty (no revocations in Round 2)
  assignments: Map<Worker, List<Task>>  // All new assignments from Phase A + Phase B
}
```

**Example:**
```
Round 2 assigns:
  w0: [c1-0, c1-1, ..., c2-0, ...] → 20 tasks assigned
  w1: [c1-10, c1-11, ..., c2-5, ...] → 19 tasks assigned
  ... (all workers)
```

### Consistency Rules

**When revoking:**
```
1. Remove task from worker's assigned list
2. Add task to unassigned pool
3. Record revocation in ClusterAssignment
```

**When assigning:**
```
1. Remove task from unassigned pool
2. Add task to worker's assigned list
3. Record assignment in ClusterAssignment
```

**Invariants maintained:**
- ✅ No task is assigned to multiple workers
- ✅ No task is both assigned and unassigned
- ✅ All tasks are accounted for (assigned + unassigned = totalTasks)

---

## Algorithm Decision Tree

Every rebalance follows this decision flow:

```
┌─────────────────────────────────────────┐
│ Rebalance Triggered                     │
│ (Worker join/leave, connector/task     │
│  changes, config updates)               │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Step 1: Build Current State             │
│                                         │
│ • configuredConnectors =                │
│     configSnapshot.connectors()         │
│ • configuredTasks =                     │
│     configSnapshot.tasks(connector)     │
│ • memberAssignments =                   │
│     current worker assignments          │
│ • Filter deleted tasks automatically    │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Step 2: Check Round 1 Triggers          │
└──────────────┬──────────────────────────┘
               │
     ┌─────────┴─────────┐
     │                   │
     ▼                   ▼
┌─────────────┐     ┌──────────────┐
│ Any worker  │     │ Any worker   │
│ has 0 tasks?│     │ > maxLimit?  │
└──────┬──────┘     └──────┬───────┘
       │                   │
   YES │               YES │
       │                   │
       └────────┬──────────┘
                │ NO to both
                ▼
        ┌───────────────┐
        │  TRIGGER      │
        │  Round 1?     │
        └───┬───────┬───┘
            │       │
         YES│       │NO
            │       │
            ▼       ▼
  ┌──────────────┐  ┌──────────────┐
  │  Round 1:    │  │  Skip        │
  │  Revoke All  │  │  Round 1     │
  └──────┬───────┘  └──────┬───────┘
         │                 │
         └────────┬────────┘
                  │
                  ▼
        ┌──────────────────┐
        │  Round 2:        │
        │  Phase A + B     │
        └─────────┬────────┘
                  │
                  ▼
        ┌──────────────────┐
        │  Cluster         │
        │  Balanced!       │
        └──────────────────┘
```

### Scenario Mappings

```
Worker Scale-Up (10→12)
  └─> Empty workers detected
      └─> Round 1: Revoke all
          └─> Round 2: Redistribute all
              └─> 2 generations

Worker Scale-Down (10→8)
  └─> No empty workers
      └─> Skip Round 1
          └─> Round 2: Assign lost tasks
              └─> 1 generation

New Connector Added
  └─> Check if creates empty workers
      ├─> YES: Round 1 + Round 2 (2 generations)
      └─> NO: Round 2 only (1 generation)

Connector Removed
  └─> Tasks filtered out automatically
      └─> Skip Round 1
          └─> Round 2: Rebalance remaining
              └─> 1 generation

Task Count Increased
  └─> No empty workers
      └─> Skip Round 1
          └─> Round 2: Assign new tasks
              └─> 1 generation

Task Count Decreased
  └─> Tasks filtered out automatically
      └─> Skip Round 1
          └─> Round 2: Rebalance if needed
              └─> 1 generation
```

---

---

## Handling Connector and Task Changes

### How Changes Flow Through the System

All connector and task changes flow through the cooperative protocol infrastructure:

```
User Action (REST API)
    ↓
ConfigBackingStore (Kafka topic)
    ↓
ConfigUpdateListener (all workers)
    ↓
needsReconfigRebalance = true
    ↓
member.requestRejoin()
    ↓
performTaskAssignment() with fresh configSnapshot
```

### Change Scenarios

#### 1. New Connector Added

**Flow:**
```
POST /connectors API
  ↓
ConfigBackingStore adds connector + tasks
  ↓
ConfigUpdateListener.onConnectorConfigUpdate()
  ↓
needsReconfigRebalance = true (connector doesn't exist yet)
  ↓
Rebalance triggered
```

**In Algorithm:**
```java
configuredConnectors = configSnapshot.connectors()  // Includes new connector
configuredTasks = configSnapshot.tasks(connector)   // Includes all new tasks
```

**Result:**
- New connector's tasks appear as "unassigned"
- Flow: Round 1 (if creates empty workers) → Round 2 (assign new tasks)
- Convergence: 1-2 generations

**Example:**
```
Before: 9 connectors, 194 tasks
Action: Add connector-10 with 50 tasks
After: 10 connectors, 244 tasks
Algorithm: Round 2 assigns 50 new tasks incrementally
```

#### 2. Connector Removed

**Flow:**
```
DELETE /connectors/{name} API
  ↓
ConfigBackingStore removes connector + tasks
  ↓
ConfigUpdateListener.onConnectorConfigRemove()
  ↓
needsReconfigRebalance = true
  ↓
Rebalance triggered
```

**In Algorithm:**
```java
configuredConnectors = configSnapshot.connectors()  // Excludes deleted connector
configuredTasks = configSnapshot.tasks(connector)   // Excludes deleted tasks

// Automatic filtering:
finalTasks.removeIf(task -> !configuredConnectors.contains(task.connector()))
```

**Result:**
- Deleted tasks automatically filtered from all worker assignments
- Remaining tasks rebalanced if needed
- Flow: Round 2 only (deleted tasks already gone)
- Convergence: 1 generation

**Example:**
```
Before: 9 connectors, 194 tasks (c9 has 1 task)
Action: Delete connector c9
After: 8 connectors, 193 tasks
Algorithm: Round 2 rebalances remaining 193 tasks if imbalance exists
```

#### 3. Task Count Increased (tasks.max ↑)

**Flow:**
```
PUT /connectors/{name}/config with higher tasks.max
  ↓
Worker calls connector.taskConfigs(maxTasks)
  ↓
Connector generates additional task configs
  ↓
ConfigBackingStore.putTaskConfigs()
  ↓
ConfigUpdateListener.onTaskConfigUpdate()
  ↓
needsReconfigRebalance = true (ALWAYS for task changes)
  ↓
Rebalance triggered
```

**In Algorithm:**
```java
configuredTasks = configSnapshot.tasks(connector)  // Includes NEW task IDs
  // e.g., connector-1: 111 tasks → 120 tasks (9 new task IDs)

unassignedTasks = configuredTasks - memberAssignments.tasks()
  // New tasks appear as unassigned
```

**Result:**
- New tasks appear as "unassigned"
- Existing tasks preserved in current assignments
- Flow: Round 2 only (incremental assignment)
- Convergence: 1 generation

**Example:**
```
Before: c1 has 111 tasks, each worker has 11-12 c1 tasks
Action: Increase c1 tasks.max from 110 to 120
After: c1 has 120 tasks, need to distribute 9 new tasks
Algorithm:
  - Recalculate: c1min=12 (was 11)
  - Round 2 Phase A: Fill workers to c1min=12
  - Round 2 Phase B: Distribute any remaining tasks
Result: Each worker has 12 c1 tasks, balanced
```

#### 4. Task Count Decreased (tasks.max ↓)

**Flow:**
```
PUT /connectors/{name}/config with lower tasks.max
  ↓
Connector generates fewer task configs
  ↓
ConfigBackingStore.putTaskConfigs()
  ↓
ConfigUpdateListener.onTaskConfigUpdate()
  ↓
Rebalance triggered
```

**In Algorithm:**
```java
configuredTasks = configSnapshot.tasks(connector)  // Fewer task IDs
  // e.g., connector-1: 111 tasks → 100 tasks (11 task IDs removed)

// Tasks with removed IDs automatically filtered out
currentTasks.removeIf(task -> !configuredTasks.contains(task.id()))
```

**Result:**
- Removed tasks filtered out automatically
- Remaining tasks preserved on current workers
- Flow: Round 2 only if rebalancing needed
- Convergence: 1 generation

**Example:**
```
Before: c1 has 111 tasks, each worker has 11-12 c1 tasks
Action: Decrease c1 tasks.max from 110 to 100
After: c1 has 100 tasks, 11 tasks removed
Algorithm:
  - Recalculate: c1min=10, c1max=10
  - Tasks c1-100 through c1-110 filtered out automatically
  - Round 2: Rebalance if any worker now has >10 c1 tasks
Result: Each worker has exactly 10 c1 tasks
```

#### 5. Connector Config Change Only (no task count change)

**Flow:**
```
PUT /connectors/{name}/config with property changes
  ↓
ConfigUpdateListener.onConnectorConfigUpdate()
  ↓
If task configs unchanged:
  └─> NO rebalance triggered (connectorConfigUpdates only)
  └─> Connector restarted locally on its worker
```

**Result:**
- Does NOT trigger rebalance
- Connector restarted locally
- Tasks continue running unchanged
- Convergence: 0 generations (local operation)

**Example:**
```
Before: connector-1 consuming from topics "topic-a,topic-b"
Action: Update connector-1 config: topics="topic-a,topic-b,topic-c"
Result: Connector-1 restarted on its worker, tasks unchanged
```

### Key Principles

1. **ConfigSnapshot is Source of Truth**
   ```
   configSnapshot.connectors() → What SHOULD exist
   configSnapshot.tasks(connector) → What SHOULD be running
   memberAssignments → What IS currently running
   ```

2. **Automatic Filtering**
   ```
   Any task in memberAssignments but NOT in configSnapshot → Filtered out
   Any task in configSnapshot but NOT in memberAssignments → Unassigned
   ```

3. **Unified Processing**
   ```
   Round 2 handles ALL unassigned tasks identically
   Source doesn't matter (scale-down, new connector, task increase, etc.)
   Same algorithm achieves consistent balance
   ```

4. **No Special Logic Needed**
   ```
   Algorithm doesn't distinguish between:
   - "Task from new connector" vs "task from existing connector"
   - "Task from scale-down" vs "task from task-count increase"
   
   All unassigned tasks → Round 2 Phase A + Phase B → Balanced
   ```

---

COMPREHENSIVE SCENARIO SUMMARY:

| Scenario | configSnapshot Changes | Round 1? | Round 2? | Generations | How It Works |
|----------|----------------------|----------|----------|-------------|--------------|
| **Worker Scale-Up** (10→12) | No change | ✅ YES (empty workers) | ✅ YES (all) | 2 | Empty workers trigger R1 full revocation → R2 redistributes all tasks |
| **Worker Scale-Down** (10→8) | No change | ❌ NO | ✅ YES (lost only) | 1 | Delayed rebalance expires → R2 incrementally assigns lost tasks |
| **New Connector** (c10 added) | +1 connector, +50 tasks | ✅ Maybe | ✅ YES | 1-2 | If empty workers: R1+R2. Else: R2 only assigns new 50 tasks |
| **Connector Removed** (c9 deleted) | -1 connector, -1 task | ❌ NO | ✅ Maybe | 1 | Tasks filtered out automatically, R2 rebalances if needed |
| **Tasks Increased** (c1: 111→120) | +9 c1 tasks | ❌ NO | ✅ YES | 1 | R2 Phase A fills higher consumerN_task_count_min, Phase B distributes 9 new tasks |
| **Tasks Decreased** (c1: 111→100) | -11 c1 tasks | ❌ NO | ✅ Maybe | 1 | Tasks filtered out, R2 rebalances remaining if imbalanced |
| **Config Change Only** | No task changes | ❌ NO | ❌ NO | 0 | Local connector restart, no rebalance triggered |

KEY PATTERNS:
1. Empty Workers → Always triggers Round 1 full revocation
2. Task/Connector Changes → Round 2 handles incrementally (no full revocation)
3. ConfigSnapshot filtering → Deleted connectors/tasks automatically removed
4. Unassigned detection → Tasks in configSnapshot but not in memberAssignments

This algorithm is designed for large-scale, multi-tenant, auto-scaled and/or spot-instance environments where:
1. Fast convergence is critical (spot instances can terminate at any time)
2. Per-consumer fairness is non-negotiable (multi-tenant SLAs)
3. Aggressive rebalancing is acceptable (tasks can handle restarts)
4. Worker churn is constant (scale-up/down happens frequently)
5. Connector/task changes happen frequently (multi-tenant workloads)


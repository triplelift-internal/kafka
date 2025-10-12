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
# Kafka Connect Distributed Mode - Global Balance Task Assignor Behavior

This document describes task allocation behavior for Kafka Connect in distributed mode across 4 scenarios, demonstrating worker-to-task assignments with strict balance requirements. The number of Workers, Consumer Groups and Tasks for those Consumer Groups are an example. The actual behavior for GlobalBalanceTaskAssignor should be capable of handling any number of Workers, Consumer Groups and Tasks.

## Balance Requirements

1. **Per-Consumer Balance:** Task count difference across workers ≤ 1 for each consumer group
2. **Global Balance:** Total task count difference across workers ≤ 1

## Baseline Configuration

**Consumer Groups and Task Counts:**

| Consumer | Tasks |
|----------|-------|
| C1 | 18 |
| C2 | 11 |
| C3 | 7 |
| C4, C5, C6 | 4 each |
| C7, C8 | 3 each |
| C9-C14 | 1 each |
| **Total** | **60** |

---

## Scenario 1: Initial Allocation New Consumer Group Deployment

### Description
A new consumer group deployment with no pre-existing task assignments. Tasks are allocated evenly among all available worker nodes.

### Behavior
- Allocate equal number of tasks among all available worker nodes, in this example 5
- Per-consumer task count difference ≤ 1
- Overall total tasks difference per worker ≤ 1

### Before State
No tasks assigned.

### After State: 5 Workers (W1-W5)

**Worker Load Summary:**
- W1: 12 tasks
- W2: 12 tasks
- W3: 12 tasks
- W4: 12 tasks
- W5: 12 tasks
- **Global Balance: Perfect (difference = 0)**

### Task Allocation Matrix

| Worker | C1 | C2 | C3 | C4 | C5 | C6 | C7 | C8 | C9 | C10 | C11 | C12 | C13 | C14 | **Total** |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-----------|
| **W1** | C1-T1, C1-T2, C1-T3, C1-T4 | C2-T6, C2-T7 | C3-T3, C3-T4 | - | C5-T1 | C6-T2 | C7-T3 | - | - | C10-T1 | - | - | - | - | **12** |
| **W2** | C1-T5, C1-T6, C1-T7, C1-T8 | C2-T8, C2-T9 | C3-T5 | C4-T1 | C5-T2 | C6-T3 | - | C8-T1 | - | - | C11-T1 | - | - | - | **12** |
| **W3** | C1-T9, C1-T10, C1-T11, C1-T12 | C2-T10, C2-T11 | C3-T6 | C4-T2 | C5-T3 | C6-T4 | - | C8-T2 | - | - | - | C12-T1 | - | - | **12** |
| **W4** | C1-T13, C1-T14, C1-T15 | C2-T1, C2-T2, C2-T3 | C3-T7 | C4-T3 | C5-T4 | - | C7-T1 | C8-T3 | - | - | - | - | C13-T1 | - | **12** |
| **W5** | C1-T16, C1-T17, C1-T18 | C2-T4, C2-T5 | C3-T1, C3-T2 | C4-T4 | - | C6-T1 | C7-T2 | - | C9-T1 | - | - | - | - | C14-T1 | **12** |

### Balance Verification

**Per-Consumer Distribution:**
- C1: [4, 4, 4, 3, 3] ✓
- C2: [2, 2, 2, 3, 2] ✓
- C3: [2, 1, 1, 1, 2] ✓
- C4: [0, 1, 1, 1, 1] ✓
- C5: [1, 1, 1, 1, 0] ✓
- C6: [1, 1, 1, 0, 1] ✓
- C7: [1, 0, 0, 1, 1] ✓
- C8: [0, 1, 1, 1, 0] ✓
- C9-C14: Single task, difference ≤ 1 ✓

All consumers have maximum difference of 1 task across workers.

---

## Scenario 2: Worker Scale Down Event

### Description
Worker scale down event where 2 nodes are removed from the cluster (W4 and W5).

### Behavior
- Rebalance only the unassigned tasks from removed workers
- Assume assigned tasks on remaining workers (W1, W2, W3) are already balanced
- Allocate among remaining nodes in a balanced way
- No task count difference > 1 for each consumer across all worker nodes
- Overall total tasks difference per worker ≤ 1
- Existing workload on W1, W2, W3 is preserved

### Before State
5 Workers (W1-W5) from Scenario 1.

### After State: 3 Workers (W1-W3)

**Workers Removed:** W4, W5

**Worker Load Summary:**
- W1: 20 tasks
- W2: 20 tasks
- W3: 20 tasks
- **Global Balance: Perfect (difference = 0)**

### Task Allocation Matrix

| Worker | C1 | C2 | C3 | C4 | C5 | C6 | C7 | C8 | C9 | C10 | C11 | C12 | C13 | C14 | **Total** |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-----------|
| **W1** | C1-T1, C1-T2, C1-T3, C1-T4, C1-T13, C1-T16 | C2-T6, C2-T7, C2-T1, C2-T4 | C3-T3, C3-T4, C3-T2 | C4-T3 | C5-T1 | C6-T2 | C7-T3 | C8-T3 | C9-T1 | C10-T1 | - | - | - | - | **20** |
| **W2** | C1-T5, C1-T6, C1-T7, C1-T8, C1-T14, C1-T17 | C2-T8, C2-T9, C2-T2, C2-T5 | C3-T5, C3-T1 | C4-T1 | C5-T2, C5-T4 | C6-T3 | C7-T1 | C8-T1 | - | - | C11-T1 | - | C13-T1 | - | **20** |
| **W3** | C1-T9, C1-T10, C1-T11, C1-T12, C1-T15, C1-T18 | C2-T10, C2-T11, C2-T3 | C3-T6, C3-T7 | C4-T2, C4-T4 | C5-T3 | C6-T4, C6-T1 | C7-T2 | C8-T2 | - | - | - | C12-T1 | - | C14-T1 | **20** |

### Balance Verification

**Per-Consumer Distribution:**
- C1: [6, 6, 6] (diff=0) ✓
- C2: [4, 4, 3] (diff=1) ✓
- C3: [3, 2, 2] (diff=1) ✓
- C4: [1, 1, 2] (diff=1) ✓
- C5: [1, 2, 1] (diff=1) ✓
- C6: [1, 1, 2] (diff=1) ✓
- C7: [1, 1, 1] (diff=0) ✓
- C8: [1, 1, 1] (diff=0) ✓
- C9-C14: Single task, difference ≤ 1 ✓

All consumers maintain maximum difference of 1 task across workers.

### Task Redistribution Details
Tasks from W4 (12 tasks) and W5 (12 tasks) were redistributed to W1, W2, and W3, maintaining perfect balance.

---

## Scenario 3: Worker Scale Up Event

### Description
Worker scale up event where 3 new nodes are added to the cluster (W4, W5, W6).

### Behavior
- At all times, task difference for each consumer group ≤ 1 among all workers
- Only trigger rebalance for consumer groups with task count > number of workers
- Allocate equal amount of tasks on each node (difference ≤ 1)
- Overall total tasks difference per worker ≤ 1
- Sort consumers by task.max count and prioritize highest task count consumers
- Revoke and move only the minimum required number of tasks to create balanced distribution

### Before State
3 Workers (W1-W3) from Scenario 2.
### After State: 6 Workers (W1-W6)

**Workers Added:** W4, W5, W6

**Worker Load Summary:**
- W1: 10 tasks
- W2: 10 tasks
- W3: 10 tasks
- W4: 10 tasks
- W5: 10 tasks
- W6: 10 tasks
- **Global Balance: Perfect (difference = 0)**

### Task Allocation Matrix

| Worker | C1 | C2 | C3 | C4 | C5 | C6 | C7 | C8 | C9 | C10 | C11 | C12 | C13 | C14 | **Total** |
|--------|----|----|----|----|----|----|----|----|----|----|----|----|----|----|-----------|
| **W1** | C1-T1, C1-T2, C1-T3 | C2-T1, C2-T2 | C3-T3 | C4-T1 | C5-T3 | - | C7-T1 | - | C9-T1 | - | - | - | - | - | **10** |
| **W2** | C1-T4, C1-T5, C1-T6 | C2-T3, C2-T4 | C3-T4 | C4-T2 | C5-T4 | - | C7-T2 | - | - | C10-T1 | - | - | - | - | **10** |
| **W3** | C1-T7, C1-T8, C1-T9 | C2-T5, C2-T6 | C3-T5 | C4-T3 | - | C6-T1 | C7-T3 | - | - | - | C11-T1 | - | - | - | **10** |
| **W4** | C1-T10, C1-T11, C1-T12 | C2-T7, C2-T8 | C3-T6 | C4-T4 | - | C6-T2 | - | C8-T1 | - | - | - | C12-T1 | - | - | **10** |
| **W5** | C1-T13, C1-T14, C1-T15 | C2-T9, C2-T10 | C3-T7 | - | C5-T1 | C6-T3 | - | C8-T2 | - | - | - | - | C13-T1 | - | **10** |
| **W6** | C1-T16, C1-T17, C1-T18 | C2-T11 | C3-T1, C3-T2 | - | C5-T2 | C6-T4 | - | C8-T3 | - | - | - | - | - | C14-T1 | **10** |

### Balance Verification

**Per-Consumer Distribution:**
- C1: [3, 3, 3, 3, 3, 3] (diff=0) ✓
- C2: [2, 2, 2, 2, 2, 1] (diff=1) ✓
- C3: [1, 1, 1, 1, 1, 2] (diff=1) ✓
- C4: [1, 1, 1, 1, 0, 0] (diff=1) ✓
- C5: [1, 1, 0, 0, 1, 1] (diff=1) ✓
- C6: [0, 0, 1, 1, 1, 1] (diff=1) ✓
- C7: [1, 1, 1, 0, 0, 0] (diff=1) ✓
- C8: [0, 0, 0, 1, 1, 1] (diff=1) ✓
- C9-C14: Single task, difference ≤ 1 ✓

All consumers maintain maximum difference of 1 task across workers.

### Rebalancing Details

**Consumers Requiring Rebalance (task count > 6):**
- C1 (18 tasks): Rebalanced from [6, 6, 6, 0, 0, 0] to [3, 3, 3, 3, 3, 3]
- C2 (11 tasks): Rebalanced from [4, 4, 3, 0, 0, 0] to [2, 2, 2, 2, 2, 1]
- C3 (7 tasks): Rebalanced from [3, 2, 2, 0, 0, 0] to [1, 1, 1, 1, 1, 2]

**Consumers Not Requiring Rebalance (task count ≤ 6):**
- C4-C14: Tasks redistributed to achieve global balance

Rebalancing prioritizes consumers with highest task counts (C1 → C2 → C3) to minimize disruption while ensuring both per-consumer and global balance constraints are satisfied.

---

## Scenario 4: Catch All & Consumer Config Change / Update

### Description
Catch all logic as well as for configuration changes or redeployment event for existing consumer groups.

### Behavior
- Trigger a full rebalance across all workers consumers and tasks ensuring balance is enforced
- Per-Consumer Balance: Task count difference across workers ≤ 1 for each consumer group
- Global Balance: Total task count difference across workers ≤ 1

### Before State
6 Workers (W1-W6) with task allocation from Scenario 3.

### After State
6 Workers (W1-W6) - **Balanced Workloads**

---


## Summary

This implementation ensures:

1. ✅ **Strict Balance Enforcement:** Task count difference never exceeds 1 (per-consumer and globally)
2. ✅ **Affinity Preservation:** Worker-to-task relationships retained during config changes (Scenario 2)
3. ✅ **Minimal Disruption:** Only unassigned tasks rebalanced during scale-down (Scenario 3)
4. ✅ **Intelligent Scale-Up:** Selective rebalancing for high-task consumers during scale-up (Scenario 4)
5. ✅ **Global Optimization:** Total workload distributed evenly across all workers in all scenarios


Total workers (total_workers) = 10
Total consumers = 9

Tasks per consumer
c1 = 111
c2 = 34
c3 = 14
c4 = 12
c5 = 11
c6 = 6
c7 = 4
c8 = 2
c9 = 1

Total Tasks = 111+34+14+12+11+6+4+2+1 = 195

Target Task Balance Per Worker = 19.5

This gives us minimum and maximum tasks per worker as
tasks_per_worker_min = floor(19.5) = 19
tasks_per_worker_max = ceiling(19.5) = 20
tasks_per_worker_max_limit = ceiling(19.5) + 1 = 21
tasks_per_worker_max_limit will tolerate up to 10% additional total tasks so that per consumer balance can be achieved.

And the Target Per Consumer Tasks allocations per worker as
c1:111 => 111/10=11.1 - c1min = floor(11.1) = 11, c1max = ceiling(11.1) = 12
c2:34  => 34/10=3.4   - c2min = floor(3.4) = 3, c2max = ceiling(3.4) = 4
c3:14  => 14/10=1.4   - c3min = floor(1.4) = 1, c3max = ceiling(1.4) = 2
c4:12  => 12/10=1.2   - c4min = floor(1.2) = 1, c4max = ceiling(1.2) = 2
c5:11  => 11/10=1.1   - c5min = floor(1.1) = 1, c5max = ceiling(1.1) = 2
c6:6   => 6/10=0.6    - c6min = floor(0.6) = 0, c6max = ceiling(0.6) = 1
c7:4   => 4/10=0.4    - c7min = floor(0.4) = 0, c7max = ceiling(0.4) = 1
c8:2   => 2/10=0.2    - c8min = floor(0.2) = 0, c8max = ceiling(0.2) = 1
c9:1   => 1/10=0.1    - c9min = floor(0.1) = 0, c9max = ceiling(0.1) = 1


Two constraints here we need to work with.
1st - each consumer will need to have a cNmin and cNmax task allocation per worker
2nd - each worker node will need to have globalMin and globalMax tasks allocation overall
For BalancedCooperativeAssignor to work properly for the calculations above it will need to consider all workers when dividing the consumer tasks for consumer balance and the total tasks across all consumers across all workers for the global balance.
Also, for consumers, it will need to consider assigned and unassigned tasks when doing the math for consumers, otherwise the math will not work.




What to do for Revocations (two-phase revocation process)

Phase 1 Revocation: Bring all workers to ≤ cNmax for each consumer
  - For each consumer:
      if consumer tasks > number of workers
        - Strategy A: Consumers with tasks > workers
        - For each worker:
          - find the actual number of running tasks for the consumer at scope
          - if running task count > cNmax then
            - Revoke tasks until count == cNmax (revoke: currentCount - cNmax tasks)
      else (consumer tasks ≤ number of workers)
        - Strategy B: Consumers with tasks ≤ workers
        - For each worker:
          - if running task count > 1 then
            - Revoke tasks until count == 1 (keep max 1 task per worker, revoke rest)

Phase 3 Revocation: Revoke from workers at cNmax to fill workers below cNmin
  - This phase only processes consumers with tasks > number of workers
  - For each consumer (where consumer tasks > number of workers):
    - Find workers below minimum:
      - Get the list of workers where running task count < cNmin
      - Calculate totalMissing = sum of (cNmin - currentCount) for all workers below cNmin
    - Find workers at maximum:
      - Get the list of workers where running task count == cNmax
      - If no workers at cNmax, fallback to workers at (cNmax - 1)
    - Revoke exactly totalMissing tasks:
      - For each worker at cNmax:
        - Revoke 1 task at a time to maintain balance
        - Continue until totalMissing tasks are revoked
  - This creates unassigned tasks that will be assigned to workers below cNmin in Phase 4



What to do for Assignment (Phase 2 and Phase 4)?

Pre-processing:
- Get all currently unassigned tasks grouped by consumer
- Sort workers by total task count (ascending - fill least loaded first)
- Sort consumers by cNmin (descending - prioritize high cNmin first)

STEP 1: Fill each worker to cNmin for each consumer
  - For each worker in sorted worker list:
    - For each consumer in sorted consumer list:
      - If currentCount < cNmin AND unassigned tasks exist for this consumer:
        - Calculate needed = cNmin - currentCount
        - Calculate available = number of unassigned tasks for this consumer
        - Assign min(needed, available) tasks to this worker
        - Note: This may exceed globalMaxLimit temporarily (per-consumer balance has priority)

STEP 2: Evenly distribute consumers with >2 unassigned tasks
  - For each consumer with >2 unassigned tasks:
    - Calculate tasksPerWorker = unassignedCount / numWorkers
    - Calculate remainder = unassignedCount % numWorkers
    - Re-sort workers by current load (including pending assignments)
    - For each worker:
      - Calculate allocation = tasksPerWorker + (1 if remainder > 0)
      - Check capacity = globalMaxLimit - currentTotal
      - Assign min(allocation, capacity, unassigned.size()) tasks
      - Decrement remainder if we gave extra task

STEP 3: Round-robin allocation to globalMax
  - While unassigned tasks remain:
    - Re-sort workers by current load each iteration
    - For each worker where currentTotal < globalMax:
      - Pick task from consumer with most unassigned tasks
      - Assign 1 task to this worker
    - Break if no progress in a round

STEP 4: Final allocation to globalMaxLimit
  - If unassigned tasks still remain:
    - First check if any workers are below globalMax (should not happen, but handle gracefully)
    - If yes, fill to globalMax first using Step 3
    - Then for remaining tasks:
      - Find worker with lowest load
      - If currentTotal < globalMaxLimit:
        - Assign 1 task
      - Else:
        - Error: Cannot assign, all workers at globalMaxLimit!


Phase Execution Order and Orchestration:

The algorithm executes phases based on the current balance state:

1. Check per-consumer balance state:
   - If any worker has currentCount > cNmax for any consumer → Execute Phase 1 (Revoke to cNmax)
   - Else if any worker has currentCount < cNmin for any consumer:
     - If unassigned tasks exist → Execute Phase 2 (Assignment to cNmin)
     - Else → Execute Phase 3 (Revoke from cNmax to fill cNmin)
   
2. After Phase 3, execute Phase 4 (Assignment) to fill the gaps

3. Check global balance state (after per-consumer balance achieved):
   - If any worker has total tasks > globalMaxLimit → Execute Global Revocation
   - Else if unassigned tasks exist → Execute Global Assignment (reuse Phase 2 logic)

4. Convergence check:
   - Per-consumer balanced: All consumers satisfy cNmin ≤ tasks per worker ≤ cNmax
   - Global balanced: All workers satisfy globalMin ≤ total tasks ≤ globalMaxLimit
   - Complete: No unassigned tasks remain

Maximum convergence rounds: 6
- Minor per-consumer imbalance: 2 rounds (Phase 1 → Phase 2)
- Major per-consumer imbalance: 4 rounds (Phase 1 → Phase 2 → Phase 3 → Phase 4)
- Both imbalances: 6 rounds (All 4 phases + 2 global)

The updated algorithm will allocate all tasks evenly.

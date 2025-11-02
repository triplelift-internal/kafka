/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An enhanced assignor that extends {@link IncrementalCooperativeAssignor} to provide balanced
 * task distribution across workers by interleaving tasks from different connectors.
 * 
 * <p>This assignor addresses the task clustering problem where tasks from the same connector
 * could be grouped on the same worker, leading to:</p>
 * <ul>
 *   <li>Hot spots when high-volume connectors cluster together</li>
 *   <li>Uneven resource utilization across the cluster</li>
 *   <li>Single points of failure for connector workloads</li>
 * </ul>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li><b>Interleaved Task Assignment:</b> Tasks are assigned in round-robin fashion across
 *       connectors, ensuring even distribution from the start</li>
 *   <li><b>Interleaved Task Revocations:</b> When rebalancing, tasks are revoked in an interleaved
 *       manner to maintain balanced distribution</li>
 *   <li><b>Load-Aware Revocations:</b> Revocations consider connector distribution to avoid
 *       re-clustering during scale-up/scale-down events</li>
 *   <li><b>Worker Join Delay:</b> Optionally delays rebalancing when new workers join to allow
 *       multiple workers to join before triggering rebalance, reducing churn in autoscaling environments</li>
 * </ul>
 * 
 * <p><b>Example:</b></p>
 * <pre>
 * Standard Assignment (clustered):
 *   Worker-1: [A-0, A-1, A-2, B-0]
 *   Worker-2: [B-1, B-2, C-0, C-1]
 * 
 * Balanced Assignment (interleaved):
 *   Worker-1: [A-0, B-0, C-0, A-3]
 *   Worker-2: [A-1, B-1, C-1, A-4]
 * </pre>
 * 
 * <p>This assignor is particularly beneficial in:</p>
 * <ul>
 *   <li>Multi-tenant environments with varying connector loads</li>
 *   <li>Clusters with high-task-count connectors</li>
 *   <li>Dynamic environments with frequent scaling events</li>
 * </ul>
 * 
 * @see IncrementalCooperativeAssignor
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-415%3A+Incremental+Cooperative+Rebalancing+in+Kafka+Connect">
 * KIP-415: Incremental Cooperative Rebalancing in Kafka Connect</a>
 */
public class BalancedCooperativeAssignor extends IncrementalCooperativeAssignor {

    private final int workerJoinDelayMs;
    private long workerJoinScheduledRebalance;
    private int workerJoinDelay;
    private Set<String> membersInPreviousRebalance;

    /**
     * Constructs a new BalancedCooperativeAssignor with interleaved assignment and revocation strategies.
     *
     * @param logContext the log context for logging
     * @param time the time implementation for scheduling
     * @param maxDelay the maximum delay for scheduled rebalances in milliseconds
     */
    public BalancedCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        this(logContext, time, maxDelay, 0);
    }

    /**
     * Constructs a new BalancedCooperativeAssignor with interleaved assignment and revocation strategies.
     *
     * @param logContext the log context for logging
     * @param time the time implementation for scheduling
     * @param maxDelay the maximum delay for scheduled rebalances in milliseconds
     * @param workerJoinDelayMs the delay in milliseconds to wait for additional workers to join before rebalancing
     */
    public BalancedCooperativeAssignor(LogContext logContext, Time time, int maxDelay, int workerJoinDelayMs) {
        super(logContext, time, maxDelay);
        this.workerJoinDelayMs = workerJoinDelayMs;
        this.workerJoinScheduledRebalance = 0;
        this.workerJoinDelay = 0;
        this.membersInPreviousRebalance = Collections.emptySet();
    }

    /**
     * Override to add worker join delay logic before performing task assignment.
     * This method detects when new workers join and delays the rebalancing process to allow
     * multiple workers to join in succession without triggering multiple rebalances.
     */
    @Override
    ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        // Detect if new workers have joined
        Set<String> currentMembers = memberAssignments.keySet();
        Set<String> newMembers = new HashSet<>(currentMembers);
        newMembers.removeAll(membersInPreviousRebalance);
        
        // Detect if workers have left
        Set<String> departedMembers = new HashSet<>(membersInPreviousRebalance);
        departedMembers.removeAll(currentMembers);
        boolean hasWorkersLeft = !departedMembers.isEmpty();
        
        boolean hasNewWorkers = !newMembers.isEmpty();
        long now = time.milliseconds();
        
        // If workers have left, we must cancel any pending worker join delay and rebalance immediately
        if (hasWorkersLeft && workerJoinScheduledRebalance > 0) {
            log.info("Detected {} worker(s) leaving: {}. Cancelling worker join delay and rebalancing immediately.",
                    departedMembers.size(), departedMembers);
            resetWorkerJoinDelay();
        }
        
        if (workerJoinDelayMs > 0 && hasNewWorkers && !hasWorkersLeft) {
            log.info("Detected {} new worker(s) joining: {}. Worker join delay is {} ms",
                    newMembers.size(), newMembers, workerJoinDelayMs);
            
            // If this is the first detection of new workers, schedule the rebalance
            if (workerJoinScheduledRebalance == 0) {
                workerJoinDelay = workerJoinDelayMs;
                workerJoinScheduledRebalance = now + workerJoinDelay;
                log.info("Scheduling rebalance for {} ms from now at timestamp {}",
                        workerJoinDelay, workerJoinScheduledRebalance);
            }
            
            // Check if the scheduled rebalance time has been reached
            if (now < workerJoinScheduledRebalance) {
                // Still waiting for more workers to join
                int remainingDelay = (int) (workerJoinScheduledRebalance - now);
                log.info("Worker join delay in progress. Delaying rebalance for {} ms. " +
                        "Scheduled rebalance time: {}, current time: {}",
                        remainingDelay, workerJoinScheduledRebalance, now);
                
                // Update members but return empty assignment to maintain current state
                membersInPreviousRebalance = currentMembers;
                
                // Return an assignment that maintains the current state without changes
                // This is critical: we need to preserve existing assignments during the delay
                return createNoChangeAssignment(memberAssignments);
            } else {
                // Delay has expired, proceed with rebalancing
                log.info("Worker join delay expired. Proceeding with rebalancing. " +
                        "Total new workers that joined during delay: {}",
                        newMembers.size());
                resetWorkerJoinDelay();
            }
        } else if (workerJoinScheduledRebalance > 0 && !hasNewWorkers && !hasWorkersLeft) {
            // No new workers and no departures, but we had a scheduled rebalance - reset it
            log.info("No new workers detected. Resetting worker join delay.");
            resetWorkerJoinDelay();
        }
        
        // Update the member tracking for next rebalance
        membersInPreviousRebalance = currentMembers;
        
        // Proceed with normal assignment logic
        return super.performTaskAssignment(configSnapshot, lastCompletedGenerationId, 
                currentGenerationId, memberAssignments);
    }

    /**
     * Creates a ClusterAssignment that maintains the current state without any changes.
     * This is used during the worker join delay period to prevent premature rebalancing.
     */
    private ClusterAssignment createNoChangeAssignment(Map<String, ConnectorsAndTasks> memberAssignments) {
        // Build the current state as "all assigned" with no new assignments or revocations
        Map<String, Collection<String>> allAssignedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> allAssignedTasks = new HashMap<>();
        
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            ConnectorsAndTasks assignment = entry.getValue();
            allAssignedConnectors.put(worker, new ArrayList<>(assignment.connectors()));
            allAssignedTasks.put(worker, new ArrayList<>(assignment.tasks()));
        }
        
        // Return a ClusterAssignment with no changes - empty new assignments and revocations
        return new ClusterAssignment(
                Collections.emptyMap(),  // newlyAssignedConnectors
                Collections.emptyMap(),  // newlyAssignedTasks
                Collections.emptyMap(),  // newlyRevokedConnectors
                Collections.emptyMap(),  // newlyRevokedTasks
                allAssignedConnectors,   // allAssignedConnectors - preserve current state
                allAssignedTasks         // allAssignedTasks - preserve current state
        );
    }

    /**
     * Resets the worker join delay state after a rebalance completes or is no longer needed.
     */
    private void resetWorkerJoinDelay() {
        if (workerJoinScheduledRebalance != 0 || workerJoinDelay != 0) {
            log.debug("Resetting worker join delay from scheduled rebalance: {} and delay: {}",
                    workerJoinScheduledRebalance, workerJoinDelay);
        }
        workerJoinScheduledRebalance = 0;
        workerJoinDelay = 0;
    }

    /**
     * Interleaves tasks from different connectors to ensure even distribution across workers.
     * This prevents task clustering where all tasks from one connector end up on the same worker.
     * 
     * <p><b>Algorithm:</b></p>
     * <ol>
     *   <li>Group tasks by connector name</li>
     *   <li>Sort connector names for deterministic ordering</li>
     *   <li>Perform round-robin selection across connectors</li>
     * </ol>
     * 
     * <p>This ensures that high-task-count connectors are spread evenly across all workers,
     * preventing hot spots and improving fault tolerance.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>
     * Input:  [A-0, A-1, A-2, A-3, B-0, B-1, C-0, C-1, C-2, C-3]
     * Output: [A-0, B-0, C-0, A-1, B-1, C-1, A-2, C-2, A-3, C-3]
     * </pre>
     *
     * @param tasks the tasks to be interleaved
     * @return a list of tasks interleaved by connector to promote even distribution
     */
    protected List<ConnectorTaskId> interleaveTasksByConnector(Collection<ConnectorTaskId> tasks) {
        if (tasks.isEmpty()) {
            return Collections.emptyList();
        }

        // Group tasks by connector name, preserving task order within each connector
        Map<String, List<ConnectorTaskId>> tasksByConnector = new HashMap<>();
        for (ConnectorTaskId task : tasks) {
            tasksByConnector.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }

        // Sort connector names for deterministic assignment across rebalances
        List<String> sortedConnectors = new ArrayList<>(tasksByConnector.keySet());
        Collections.sort(sortedConnectors);

        // Create iterators for each connector's task list
        Map<String, Iterator<ConnectorTaskId>> iterators = new HashMap<>();
        for (String connector : sortedConnectors) {
            iterators.put(connector, tasksByConnector.get(connector).iterator());
        }

        // Interleave tasks using round-robin across connectors
        List<ConnectorTaskId> interleavedTasks = new ArrayList<>(tasks.size());
        boolean hasMore = true;
        while (hasMore) {
            hasMore = false;
            for (String connector : sortedConnectors) {
                Iterator<ConnectorTaskId> it = iterators.get(connector);
                if (it.hasNext()) {
                    interleavedTasks.add(it.next());
                    hasMore = true;
                }
            }
        }

        log.warn("BalancedCooperativeAssignor - Interleaved {} tasks from {} connectors for balanced distribution",
                interleavedTasks.size(), sortedConnectors.size());
        return interleavedTasks;
    }

    /**
     * Performs task assignment using interleaved distribution to prevent clustering.
     * 
     * <p>This method overrides the parent implementation to apply interleaving before
     * the round-robin assignment, ensuring tasks from different connectors are evenly
     * distributed across workers from the start.</p>
     * 
     * @param workerAssignment the current worker assignment; assigned tasks are added to this list
     * @param tasks the tasks to be assigned
     */
    @Override
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        // Interleave tasks by connector to promote even distribution across workers
        Collection<ConnectorTaskId> tasksToAssign = interleaveTasksByConnector(tasks);
        
        log.warn("BalancedCooperativeAssignor - Assigning {} interleaved tasks to {} workers for balanced distribution",
                tasksToAssign.size(), workerAssignment.size());
        // Delegate to parent's round-robin assignment logic with interleaved tasks
        super.assignTasks(workerAssignment, tasksToAssign);
    }

    /**
     * Performs load-balancing revocations with interleaved selection to maintain balanced distribution.
     * 
     * <p>This method overrides the parent implementation to ensure that when workers join the cluster
     * and tasks need to be revoked from overloaded workers, the revocations are spread across
     * different connectors rather than clustering revocations from the same connector.</p>
     * 
     * <p><b>Why This Matters:</b></p>
     * <ul>
     *   <li>Without interleaved revocations, all tasks from one connector could be revoked and
     *       reassigned to the same new worker, defeating the interleaved assignment strategy</li>
     *   <li>Interleaved revocations maintain balanced distribution through cluster scaling events</li>
     *   <li>Prevents re-clustering during worker join/leave scenarios</li>
     * </ul>
     * 
     * @param configured the set of configured connectors and tasks across the entire cluster
     * @param workers the workers in the cluster, whose assignments should not include any deleted
     *                or duplicated connectors or tasks that are already due to be revoked
     * @return which connectors and tasks should be revoked from which workers; never null, but may
     *         be empty if no load-balancing revocations are necessary or possible
     */
    @Override
    protected Map<String, ConnectorsAndTasks> performLoadBalancingRevocations(
            ConnectorsAndTasks configured,
            Collection<WorkerLoad> workers
    ) {
        workers.forEach(wl -> log.warn(
                "BalancedCooperativeAssignor - Per worker current load size; worker: {} connectors: {} tasks: {}",
                wl.worker(), wl.connectorsSize(), wl.tasksSize()));

        if (workers.stream().allMatch(WorkerLoad::isEmpty)) {
            log.warn("BalancedCooperativeAssignor - No load-balancing revocations required; all workers are either new "
                    + "or will have all currently-assigned connectors and tasks revoked during this round"
            );
            return Collections.emptyMap();
        }
        if (configured.isEmpty()) {
            log.warn("BalancedCooperativeAssignor - No load-balancing revocations required; no connectors are currently configured on this cluster");
            return Collections.emptyMap();
        }

        Map<String, ConnectorsAndTasks.Builder> result = new HashMap<>();

        // For connectors, use the standard (non-interleaved) revocation strategy
        // since connectors are typically fewer and less prone to clustering issues
        Map<String, Set<String>> connectorRevocations = loadBalancingRevocations(
                "connector",
                configured.connectors().size(),
                workers,
                WorkerLoad::connectors
        );

        // For tasks, use interleaved revocation selection to maintain balanced distribution
        Map<String, Set<ConnectorTaskId>> taskRevocations = interleavedLoadBalancingRevocations(
                configured.tasks().size(),
                workers
        );

        connectorRevocations.forEach((worker, revoked) ->
                result.computeIfAbsent(worker, w -> new ConnectorsAndTasks.Builder()).addConnectors(revoked)
        );
        taskRevocations.forEach((worker, revoked) ->
                result.computeIfAbsent(worker, w -> new ConnectorsAndTasks.Builder()).addTasks(revoked)
        );

        return buildAll(result);
    }

    /**
     * Performs interleaved task revocations to maintain balanced distribution during rebalancing.
     * 
     * <p>This method calculates which tasks should be revoked from overloaded workers, but instead
     * of selecting tasks sequentially (which could cluster all tasks from one connector), it
     * selects tasks in an interleaved manner across different connectors.</p>
     * 
     * <p><b>Algorithm:</b></p>
     * <ol>
     *   <li>Calculate how many tasks each worker should have (min + optional extra)</li>
     *   <li>Identify overloaded workers that need to revoke tasks</li>
     *   <li>For each overloaded worker, group their tasks by connector</li>
     *   <li>Select tasks to revoke using round-robin across connectors</li>
     * </ol>
     * 
     * <p><b>Example:</b></p>
     * <pre>
     * Worker has: [A-0, A-1, A-2, B-0, B-1, C-0] (6 tasks, needs to revoke 2)
     * 
     * Sequential revocation would revoke: [A-0, A-1]
     * Interleaved revocation revokes: [A-0, B-0]
     * 
     * Result: Better distribution maintained across connectors
     * </pre>
     * 
     * @param totalToAllocate the total number of tasks to allocate across all workers
     * @param workers the collection of workers with their current task assignments
     * @return a map of worker IDs to sets of tasks that should be revoked from each worker
     */
    protected Map<String, Set<ConnectorTaskId>> interleavedLoadBalancingRevocations(
            int totalToAllocate,
            Collection<WorkerLoad> workers
    ) {
        int totalWorkers = workers.size();
        // The minimum tasks that should be assigned to each worker
        int minAllocatedPerWorker = totalToAllocate / totalWorkers;
        // How many workers will have exactly one extra task
        int workersToAllocateExtra = totalToAllocate % totalWorkers;

        // Check if already balanced
        long workersAllocatedMinimum = workers.stream()
                .map(WorkerLoad::tasksSize)
                .filter(n -> n == minAllocatedPerWorker)
                .count();
        long workersAllocatedSingleExtra = workers.stream()
                .map(WorkerLoad::tasksSize)
                .filter(n -> n == minAllocatedPerWorker + 1)
                .count();
        
        if (workersAllocatedSingleExtra == workersToAllocateExtra
                && workersAllocatedMinimum + workersAllocatedSingleExtra == totalWorkers) {
            log.warn("BalancedCooperativeAssignor - No load-balancing task revocations required; the current allocations, when combined with any newly-created tasks, should be balanced");

            return Collections.emptyMap();
        }

        Map<String, Set<ConnectorTaskId>> result = new HashMap<>();
        int allocatedExtras = 0;

        // Sort workers by task count (ascending) to ensure deterministic allocation
        // Workers with fewer tasks get the "extra" slots first, ensuring eventual convergence
        List<WorkerLoad> sortedWorkers = new ArrayList<>(workers);
        sortedWorkers.sort(WorkerLoad.taskComparator());

        // Calculate which tasks to revoke from each overloaded worker
        for (WorkerLoad worker : sortedWorkers) {
            int currentTaskCount = worker.tasksSize();
            if (currentTaskCount <= minAllocatedPerWorker) {
                // This worker isn't overloaded
                continue;
            }

            int maxAllocationForWorker;
            if (allocatedExtras < workersToAllocateExtra) {
                // This worker gets one of the extra task slots
                allocatedExtras++;
                if (currentTaskCount == minAllocatedPerWorker + 1) {
                    // Already at the right allocation
                    continue;
                }
                maxAllocationForWorker = minAllocatedPerWorker + 1;
            } else {
                maxAllocationForWorker = minAllocatedPerWorker;
            }

            // Calculate how many tasks to revoke from this worker
            int numToRevoke = currentTaskCount - maxAllocationForWorker;
            
            // Select tasks to revoke in an interleaved manner
            Set<ConnectorTaskId> revokedFromWorker = selectInterleavedTasksToRevoke(
                    worker.tasks(), 
                    numToRevoke
            );
            
            if (!revokedFromWorker.isEmpty()) {
                result.put(worker.worker(), revokedFromWorker);
                log.warn("BalancedCooperativeAssignor - Revoking {} tasks from worker {} in interleaved manner to maintain balanced distribution",
                        revokedFromWorker.size(), worker.worker());
            }
        }

        return result;
    }

    /**
     * Selects tasks to revoke from a worker in an interleaved manner across connectors.
     * 
     * <p>This ensures that revocations are spread across different connectors rather than
     * taking all tasks from the same connector, which maintains balanced distribution
     * even during rebalancing events.</p>
     * 
     * @param workerTasks the current tasks assigned to the worker
     * @param numToRevoke the number of tasks that need to be revoked
     * @return a set of tasks selected for revocation in an interleaved manner
     */
    protected Set<ConnectorTaskId> selectInterleavedTasksToRevoke(
            Collection<ConnectorTaskId> workerTasks,
            int numToRevoke
    ) {
        if (numToRevoke <= 0 || workerTasks.isEmpty()) {
            return Collections.emptySet();
        }

        // Group tasks by connector
        Map<String, List<ConnectorTaskId>> tasksByConnector = new HashMap<>();
        for (ConnectorTaskId task : workerTasks) {
            tasksByConnector.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }

        // Sort connector names for deterministic selection
        List<String> sortedConnectors = new ArrayList<>(tasksByConnector.keySet());
        Collections.sort(sortedConnectors);

        // Create iterators for each connector's task list
        Map<String, Iterator<ConnectorTaskId>> iterators = new HashMap<>();
        for (String connector : sortedConnectors) {
            iterators.put(connector, tasksByConnector.get(connector).iterator());
        }

        // Select tasks in round-robin fashion across connectors
        Set<ConnectorTaskId> tasksToRevoke = new LinkedHashSet<>();
        while (tasksToRevoke.size() < numToRevoke) {
            boolean addedAny = false;
            for (String connector : sortedConnectors) {
                if (tasksToRevoke.size() >= numToRevoke) {
                    break;
                }
                Iterator<ConnectorTaskId> it = iterators.get(connector);
                if (it.hasNext()) {
                    tasksToRevoke.add(it.next());
                    addedAny = true;
                }
            }
            // If we couldn't add any tasks in this round, break to avoid infinite loop
            if (!addedAny) {
                break;
            }
        }

        return tasksToRevoke;
    }

    /**
     * Helper method to build all ConnectorsAndTasks from their builders.
     * This mirrors the private method in the parent class for convenience.
     */
    private static <K> Map<K, ConnectorsAndTasks> buildAll(Map<K, ConnectorsAndTasks.Builder> builders) {
        Map<K, ConnectorsAndTasks> result = new HashMap<>();
        for (Map.Entry<K, ConnectorsAndTasks.Builder> entry : builders.entrySet()) {
            result.put(entry.getKey(), entry.getValue().build());
        }
        return result;
    }
}

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
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    /**
     * Constructs a new BalancedCooperativeAssignor with interleaved assignment and revocation strategies.
     *
     * @param logContext the log context for logging
     * @param time the time implementation for scheduling
     * @param maxDelay the maximum delay for scheduled rebalances in milliseconds
     */
    public BalancedCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        super(logContext, time, maxDelay);
    }

    /**
     * Performs connector-aware task assignment to ensure even distribution of tasks from each
     * connector across all workers. This prevents task clustering where many tasks from the same
     * connector end up on the same worker.
     * 
     * <p><b>Algorithm:</b></p>
     * <ol>
     *   <li>Group tasks by connector for deterministic processing</li>
     *   <li>For each task, find the worker with:
     *     <ul>
     *       <li>Fewest tasks from that specific connector</li>
     *       <li>If tied, pick the worker with fewest total tasks</li>
     *       <li>If still tied, pick deterministically by worker name</li>
     *     </ul>
     *   </li>
     *   <li>Track per-connector-per-worker counts to maintain balance</li>
     * </ol>
     * 
     * <p><b>Example:</b></p>
     * <pre>
     * Before: Worker1 has [A-0, A-1, A-2], Worker2 has [B-0], Worker3 has []
     * Assigning: [A-3, A-4, B-1, B-2]
     * 
     * A-3 -> Worker3 (0 A tasks) instead of Worker1 (3 A tasks)
     * A-4 -> Worker2 (0 A tasks) instead of Worker1 (3 A tasks)
     * B-1 -> Worker1 (0 B tasks) instead of Worker2 (1 B task)
     * B-2 -> Worker3 (0 B tasks) instead of Worker2 (1 B task)
     * 
     * Result: Even distribution of A and B tasks across all workers
     * </pre>
     * 
     * <p>This connector-aware approach ensures that:</p>
     * <ul>
     *   <li>High-task-count connectors spread evenly across all workers</li>
     *   <li>No worker becomes a hotspot for any single connector</li>
     *   <li>Load balancing considers both per-connector and overall distribution</li>
     * </ul>
     *
     * @param workerAssignment the current worker assignment; assigned tasks are added to this list
     * @param tasks the tasks to be assigned
     */
    @Override
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        if (tasks.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        log.warn("BalancedCooperativeAssignor - Assigning {} tasks to {} workers using connector-aware distribution",
                tasks.size(), workerAssignment.size());

        // Track how many tasks from each connector are currently on each worker
        // Map: workerName -> (connectorName -> taskCount)
        Map<String, Map<String, Integer>> workerConnectorTaskCounts = new HashMap<>();
        for (WorkerLoad worker : workerAssignment) {
            Map<String, Integer> connectorCounts = new HashMap<>();
            for (ConnectorTaskId task : worker.tasks()) {
                connectorCounts.merge(task.connector(), 1, Integer::sum);
            }
            workerConnectorTaskCounts.put(worker.worker(), connectorCounts);
        }

        // Group tasks by connector and sort for deterministic assignment
        Map<String, List<ConnectorTaskId>> tasksByConnector = new HashMap<>();
        for (ConnectorTaskId task : tasks) {
            tasksByConnector.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }
        
        // Sort tasks within each connector by task ID for determinism
        for (List<ConnectorTaskId> connectorTaskList : tasksByConnector.values()) {
            connectorTaskList.sort((t1, t2) -> Integer.compare(t1.task(), t2.task()));
        }
        
        List<String> sortedConnectors = new ArrayList<>(tasksByConnector.keySet());
        Collections.sort(sortedConnectors);

        // Process connectors in sorted order for determinism
        for (String connector : sortedConnectors) {
            List<ConnectorTaskId> connectorTasks = tasksByConnector.get(connector);
            
            log.warn("BalancedCooperativeAssignor - Assigning {} tasks from connector '{}' using connector-aware selection",
                    connectorTasks.size(), connector);

            // For each task of this connector, find the best worker
            for (ConnectorTaskId task : connectorTasks) {
                WorkerLoad bestWorker = findBestWorkerForTask(
                        workerAssignment, 
                        task.connector(), 
                        workerConnectorTaskCounts
                );

                // Assign the task to the best worker
                bestWorker.assign(task);

                // Update the tracking map
                workerConnectorTaskCounts.get(bestWorker.worker())
                        .merge(task.connector(), 1, Integer::sum);

                log.debug("BalancedCooperativeAssignor - Assigned task {} to worker {} " +
                        "(connector task count: {}, total tasks: {})",
                        task, bestWorker.worker(),
                        workerConnectorTaskCounts.get(bestWorker.worker()).get(task.connector()),
                        bestWorker.tasksSize());
            }
        }

        log.warn("BalancedCooperativeAssignor - Completed connector-aware task assignment. " +
                "Final worker loads: {}", workerAssignment.stream()
                .map(w -> w.worker() + "=" + w.tasksSize())
                .collect(Collectors.joining(", ")));
    }

    /**
     * Finds the best worker to assign a task from a specific connector.
     * 
     * <p>Selection criteria (in priority order):</p>
     * <ol>
     *   <li>Worker with fewest tasks from this connector (prevents clustering)</li>
     *   <li>Among those tied, worker with fewest total tasks (load balancing)</li>
     *   <li>Among those still tied, pick deterministically by worker name (stability)</li>
     * </ol>
     * 
     * @param workers the list of workers to choose from
     * @param connector the connector name for the task being assigned
     * @param workerConnectorTaskCounts current per-worker per-connector task counts
     * @return the best worker to assign the task to
     */
    private WorkerLoad findBestWorkerForTask(
            List<WorkerLoad> workers,
            String connector,
            Map<String, Map<String, Integer>> workerConnectorTaskCounts
    ) {
        WorkerLoad bestWorker = null;
        int minConnectorTasks = Integer.MAX_VALUE;
        int minTotalTasks = Integer.MAX_VALUE;

        for (WorkerLoad worker : workers) {
            int connectorTaskCount = workerConnectorTaskCounts.get(worker.worker())
                    .getOrDefault(connector, 0);
            int totalTaskCount = worker.tasksSize();

            // Compare by connector task count first (primary criterion)
            if (connectorTaskCount < minConnectorTasks) {
                bestWorker = worker;
                minConnectorTasks = connectorTaskCount;
                minTotalTasks = totalTaskCount;
            } else if (connectorTaskCount == minConnectorTasks) {
                // If tied on connector tasks, compare by total tasks (secondary criterion)
                if (totalTaskCount < minTotalTasks) {
                    bestWorker = worker;
                    minTotalTasks = totalTaskCount;
                } else if (totalTaskCount == minTotalTasks) {
                    // If still tied, pick deterministically by worker name (tertiary criterion)
                    if (bestWorker == null || worker.worker().compareTo(bestWorker.worker()) < 0) {
                        bestWorker = worker;
                    }
                }
            }
        }

        return bestWorker;
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

        // Calculate cluster-wide connector task counts for balanced revocation
        Map<String, Integer> clusterConnectorTaskCounts = new HashMap<>();
        for (WorkerLoad worker : workers) {
            for (ConnectorTaskId task : worker.tasks()) {
                clusterConnectorTaskCounts.merge(task.connector(), 1, Integer::sum);
            }
        }

        // For tasks, use interleaved revocation selection to maintain balanced distribution
        Map<String, Set<ConnectorTaskId>> taskRevocations = interleavedLoadBalancingRevocations(
                configured.tasks().size(),
                workers,
                clusterConnectorTaskCounts
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
     * @param clusterConnectorTaskCounts cluster-wide task counts per connector for balanced revocation
     * @return a map of worker IDs to sets of tasks that should be revoked from each worker
     */
    protected Map<String, Set<ConnectorTaskId>> interleavedLoadBalancingRevocations(
            int totalToAllocate,
            Collection<WorkerLoad> workers,
            Map<String, Integer> clusterConnectorTaskCounts
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
            
            // Select tasks to revoke in an interleaved manner with balanced allocation for high-volume connectors
            Set<ConnectorTaskId> revokedFromWorker = selectInterleavedTasksToRevoke(
                    worker.tasks(), 
                    numToRevoke,
                    totalWorkers,
                    clusterConnectorTaskCounts
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
     * Selects tasks to revoke from a worker using a two-tier strategy:
     * 
     * <p><b>Tier 1: High-Volume Connectors</b> - For connectors with task count > worker count,
     * applies balanced allocation math to determine how many tasks this worker should keep,
     * ensuring proportional revocations.</p>
     * 
     * <p><b>Tier 2: Round-Robin</b> - For remaining tasks or connectors with task count <= worker count,
     * uses round-robin selection across connectors to maintain balanced distribution.</p>
     * 
     * <p><b>Example:</b></p>
     * <pre>
     * Cluster: 6 workers
     * Worker has: [Connector-A: 4 tasks, Connector-B: 2 tasks, Connector-C: 1 task]
     * Connector-A cluster-wide: 10 tasks (high-volume)
     * Connector-B cluster-wide: 4 tasks (low-volume)
     * Connector-C cluster-wide: 2 tasks (low-volume)
     * Need to revoke: 3 tasks
     * 
     * Tier 1 (High-Volume - Connector-A with 10 tasks):
     *   minPerWorker = 10 / 6 = 1
     *   workersWithExtra = 10 % 6 = 4
     *   This worker currently has 4 A-tasks, should keep max 2 (1 + 1 extra)
     *   Target revocations from A: 4 - 2 = 2 tasks
     *   Revoke: [A-0, A-1] (2 tasks)
     * 
     * Tier 2 (Round-Robin for remaining 1 task):
     *   Round-robin through B, C
     *   Revoke: [B-0] (1 task)
     * 
     * Final revocations: [A-0, A-1, B-0]
     * Result: High-volume connector balanced first, then even distribution
     * </pre>
     * 
     * @param workerTasks the current tasks assigned to the worker
     * @param numToRevoke the number of tasks that need to be revoked
     * @param totalWorkers the total number of workers in the cluster
     * @param clusterConnectorTaskCounts cluster-wide task counts per connector
     * @return a set of tasks selected for revocation using the two-tier strategy
     */
    protected Set<ConnectorTaskId> selectInterleavedTasksToRevoke(
            Collection<ConnectorTaskId> workerTasks,
            int numToRevoke,
            int totalWorkers,
            Map<String, Integer> clusterConnectorTaskCounts
    ) {
        if (numToRevoke <= 0 || workerTasks.isEmpty()) {
            return Collections.emptySet();
        }

        // Group tasks by connector
        Map<String, List<ConnectorTaskId>> tasksByConnector = new HashMap<>();
        for (ConnectorTaskId task : workerTasks) {
            tasksByConnector.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }

        Set<ConnectorTaskId> tasksToRevoke = new LinkedHashSet<>();

        // TIER 1: Handle high-volume connectors first
        revokeFromHighVolumeConnectors(
                tasksToRevoke, 
                tasksByConnector, 
                numToRevoke, 
                totalWorkers, 
                clusterConnectorTaskCounts
        );

        // TIER 2: If we still need to revoke more, use round-robin
        if (tasksToRevoke.size() < numToRevoke) {
            revokeRoundRobin(tasksToRevoke, tasksByConnector, numToRevoke);
        }

        log.debug("BalancedCooperativeAssignor - Final revocation selection: {} tasks from connectors: {}",
                tasksToRevoke.size(),
                tasksToRevoke.stream()
                        .collect(Collectors.groupingBy(ConnectorTaskId::connector, Collectors.counting())));

        return tasksToRevoke;
    }

    /**
     * Revokes tasks from high-volume connectors (task count > worker count) using balanced allocation math.
     */
    private void revokeFromHighVolumeConnectors(
            Set<ConnectorTaskId> tasksToRevoke,
            Map<String, List<ConnectorTaskId>> tasksByConnector,
            int numToRevoke,
            int totalWorkers,
            Map<String, Integer> clusterConnectorTaskCounts
    ) {
        List<String> highVolumeConnectors = identifyHighVolumeConnectors(
                tasksByConnector, 
                totalWorkers, 
                clusterConnectorTaskCounts
        );

        log.debug("BalancedCooperativeAssignor - High-volume connectors (task count > workers): {}",
                highVolumeConnectors.stream()
                        .map(c -> c + "(" + clusterConnectorTaskCounts.get(c) + " tasks)")
                        .collect(Collectors.joining(", ")));

        // Apply balanced allocation math to high-volume connectors
        for (String connector : highVolumeConnectors) {
            if (tasksToRevoke.size() >= numToRevoke) {
                break;
            }

            List<ConnectorTaskId> connectorTasks = tasksByConnector.get(connector);
            int workerCurrentCount = connectorTasks.size();
            int clusterTaskCount = clusterConnectorTaskCounts.get(connector);

            int maxToKeep = calculateMaxTasksToKeep(clusterTaskCount, totalWorkers);

            // If this worker is overloaded for this connector, revoke excess
            if (workerCurrentCount > maxToKeep) {
                int toRevokeFromConnector = Math.min(
                        workerCurrentCount - maxToKeep,
                        numToRevoke - tasksToRevoke.size()
                );

                log.debug("BalancedCooperativeAssignor - Connector {} (high-volume): cluster={}, worker={}, " +
                        "maxToKeep={}, revoking={} tasks",
                        connector, clusterTaskCount, workerCurrentCount, maxToKeep, toRevokeFromConnector);

                // Revoke the first N tasks from this connector
                for (int i = 0; i < toRevokeFromConnector && i < connectorTasks.size(); i++) {
                    tasksToRevoke.add(connectorTasks.get(i));
                }
            }
        }
    }

    /**
     * Identifies high-volume connectors (task count > worker count) and sorts them deterministically.
     */
    private List<String> identifyHighVolumeConnectors(
            Map<String, List<ConnectorTaskId>> tasksByConnector,
            int totalWorkers,
            Map<String, Integer> clusterConnectorTaskCounts
    ) {
        List<String> highVolumeConnectors = new ArrayList<>();
        for (String connector : tasksByConnector.keySet()) {
            int clusterTaskCount = clusterConnectorTaskCounts.getOrDefault(connector, 0);
            if (clusterTaskCount > totalWorkers) {
                highVolumeConnectors.add(connector);
            }
        }

        // Sort by cluster task count descending for determinism
        highVolumeConnectors.sort((c1, c2) -> {
            int count1 = clusterConnectorTaskCounts.get(c1);
            int count2 = clusterConnectorTaskCounts.get(c2);
            int countCompare = Integer.compare(count2, count1);
            if (countCompare != 0) {
                return countCompare;
            }
            return c1.compareTo(c2);
        });

        return highVolumeConnectors;
    }

    /**
     * Calculates the maximum number of tasks a worker should keep for a connector
     * based on balanced allocation math.
     */
    private int calculateMaxTasksToKeep(int clusterTaskCount, int totalWorkers) {
        int minAllocatedPerWorkerForConnector = clusterTaskCount / totalWorkers;
        int workersToAllocateExtra = clusterTaskCount % totalWorkers;
        
        // This worker should keep at most: min + (possibly 1 extra)
        return workersToAllocateExtra > 0 
                ? minAllocatedPerWorkerForConnector + 1 
                : minAllocatedPerWorkerForConnector;
    }

    /**
     * Revokes remaining tasks using round-robin selection across connectors.
     */
    private void revokeRoundRobin(
            Set<ConnectorTaskId> tasksToRevoke,
            Map<String, List<ConnectorTaskId>> tasksByConnector,
            int numToRevoke
    ) {
        log.debug("BalancedCooperativeAssignor - After high-volume revocations: {}/{} tasks revoked. " +
                "Using round-robin for remaining {} tasks",
                tasksToRevoke.size(), numToRevoke, numToRevoke - tasksToRevoke.size());

        // Build list of connectors sorted by task count (descending), then alphabetically
        List<String> sortedConnectors = new ArrayList<>(tasksByConnector.keySet());
        sortedConnectors.sort((c1, c2) -> {
            int count1 = tasksByConnector.get(c1).size();
            int count2 = tasksByConnector.get(c2).size();
            int countCompare = Integer.compare(count2, count1);
            if (countCompare != 0) {
                return countCompare;
            }
            return c1.compareTo(c2);
        });

        // Create iterators, skipping already-revoked tasks
        Map<String, Iterator<ConnectorTaskId>> iterators = createIteratorsSkippingRevoked(
                tasksByConnector, 
                tasksToRevoke
        );

        // Select tasks in round-robin fashion
        selectTasksRoundRobin(tasksToRevoke, sortedConnectors, iterators, numToRevoke);
    }

    /**
     * Creates iterators for connectors, skipping tasks that are already marked for revocation.
     */
    private Map<String, Iterator<ConnectorTaskId>> createIteratorsSkippingRevoked(
            Map<String, List<ConnectorTaskId>> tasksByConnector,
            Set<ConnectorTaskId> tasksToRevoke
    ) {
        Map<String, Iterator<ConnectorTaskId>> iterators = new HashMap<>();
        for (Map.Entry<String, List<ConnectorTaskId>> entry : tasksByConnector.entrySet()) {
            List<ConnectorTaskId> availableTasks = new ArrayList<>();
            for (ConnectorTaskId task : entry.getValue()) {
                if (!tasksToRevoke.contains(task)) {
                    availableTasks.add(task);
                }
            }
            if (!availableTasks.isEmpty()) {
                iterators.put(entry.getKey(), availableTasks.iterator());
            }
        }
        return iterators;
    }

    /**
     * Selects tasks in round-robin fashion from the iterators until target is reached.
     */
    private void selectTasksRoundRobin(
            Set<ConnectorTaskId> tasksToRevoke,
            List<String> sortedConnectors,
            Map<String, Iterator<ConnectorTaskId>> iterators,
            int numToRevoke
    ) {
        while (tasksToRevoke.size() < numToRevoke) {
            boolean addedAny = false;
            for (String connector : sortedConnectors) {
                if (tasksToRevoke.size() >= numToRevoke) {
                    break;
                }
                Iterator<ConnectorTaskId> it = iterators.get(connector);
                if (it != null && it.hasNext()) {
                    tasksToRevoke.add(it.next());
                    addedAny = true;
                }
            }
            if (!addedAny) {
                break;
            }
        }
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

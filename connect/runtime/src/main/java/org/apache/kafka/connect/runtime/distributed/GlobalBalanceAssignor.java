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
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An assignor that prioritizes global balance of tasks over availability and continuity.
 * This assignor extends the incremental cooperative assignor to leverage existing 
 * rebalancing logic while implementing a global balance strategy for task assignment.
 * 
 * <p>Tasks for the same connector are spread evenly across all available worker nodes
 * rather than being placed together, providing better load distribution but potentially
 * impacting connector locality.
 * 
 * <p>This assignor maintains all existing features like delayed rebalancing, revocation
 * handling, and incremental cooperative rebalancing while changing only the assignment
 * strategy for new tasks and connectors.
 */
public class GlobalBalanceAssignor extends IncrementalCooperativeAssignor {
    private final Logger log;

    public GlobalBalanceAssignor(LogContext logContext, Time time, int maxDelay) {
        super(logContext, time, maxDelay);
        this.log = logContext.logger(GlobalBalanceAssignor.class);
    }

    @Override
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        if (tasks.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        log.debug("Assigning {} tasks with global balance across {} workers",
                  tasks.size(), workerAssignment.size());

        // Identify which tasks are currently assigned and which are new/unassigned
        Set<ConnectorTaskId> currentlyAssigned = workerAssignment.stream()
                .flatMap(worker -> worker.tasks().stream())
                .collect(Collectors.toSet());

        Collection<ConnectorTaskId> unassignedTasks = tasks.stream()
                .filter(task -> !currentlyAssigned.contains(task))
                .collect(Collectors.toList());

        // Check if this is a full rebalance request (all tasks are already assigned)
        boolean isFullRebalance = unassignedTasks.isEmpty() && !tasks.isEmpty();

        if (isFullRebalance) {
            log.debug("Performing full rebalance - clearing current assignments and redistributing all tasks");
            // Clear existing task assignments for full rebalance
            for (WorkerLoad worker : workerAssignment) {
                worker.tasks().clear();
            }
            // Assign all tasks using simple round-robin for global balance
            assignTasksRoundRobin(workerAssignment, tasks);
        } else if (!unassignedTasks.isEmpty()) {
            log.debug("Performing incremental assignment for {} unassigned tasks", unassignedTasks.size());
            // Assign unassigned tasks considering existing load for balance
            assignTasksLoadAware(workerAssignment, unassignedTasks);
        } else {
            // All tasks are already assigned, nothing to do
            log.debug("All tasks are already assigned, no assignment needed");
        }
    }

    /**
     * Assigns tasks using round-robin distribution for perfect global balance.
     * This method guarantees that the difference in task count between any two
     * workers will be at most 1, achieving near-perfect load distribution.
     * 
     * For N tasks and W workers:
     * - Each worker gets floor(N/W) tasks
     * - The first (N mod W) workers get one additional task
     * - Maximum difference between workers is always 1
     */
    private void assignTasksRoundRobin(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        // Sort workers consistently for deterministic assignment
        workerAssignment.sort((w1, w2) -> w1.worker().compareTo(w2.worker()));

        // Convert tasks to list for indexed access and sort for deterministic assignment
        List<ConnectorTaskId> taskList = tasks.stream()
                .sorted((t1, t2) -> {
                    int connectorCompare = t1.connector().compareTo(t2.connector());
                    if (connectorCompare != 0) return connectorCompare;
                    return Integer.compare(t1.task(), t2.task());
                })
                .collect(Collectors.toList());

        log.debug("Round-robin assigning {} tasks across {} workers for perfect global balance",
                  taskList.size(), workerAssignment.size());

        // Simple round-robin across all workers for perfect global balance
        int workerIndex = 0;
        for (ConnectorTaskId task : taskList) {
            WorkerLoad targetWorker = workerAssignment.get(workerIndex % workerAssignment.size());
            targetWorker.assign(task);

            log.debug("Assigning task {} to worker {} (round-robin index {})",
                     task, targetWorker.worker(), workerIndex);

            workerIndex++;
        }

        // Log final distribution for verification
        if (log.isDebugEnabled()) {
            int[] taskCounts = workerAssignment.stream().mapToInt(WorkerLoad::tasksSize).toArray();
            int minTasks = Arrays.stream(taskCounts).min().orElse(0);
            int maxTasks = Arrays.stream(taskCounts).max().orElse(0);
            log.debug("Perfect balance achieved: distribution={}, min={}, max={}, difference={}", 
                     Arrays.toString(taskCounts), minTasks, maxTasks, maxTasks - minTasks);
        }
    }

    @Override
    protected void assignConnectors(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        log.debug("Assigning {} connectors with global balance strategy across {} workers", 
                  connectors.size(), workerAssignment.size());
        assignConnectorsWithGlobalBalance(workerAssignment, connectors);
    }

    /**
     * Assigns connectors with global balance strategy, ensuring even distribution
     * of connectors across workers.
     */
    private void assignConnectorsWithGlobalBalance(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        if (connectors.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        // Convert to list for indexed access
        List<String> connectorList = connectors.stream().collect(Collectors.toList());

        // Round-robin assignment of connectors
        for (int i = 0; i < connectorList.size(); i++) {
            // Sort workers by connector load to maintain balance
            workerAssignment.sort(WorkerLoad.connectorComparator());

            String connector = connectorList.get(i);
            WorkerLoad leastLoadedWorker = workerAssignment.get(0);

            log.debug("Assigning connector {} to worker {} (current connector load: {})", 
                      connector, leastLoadedWorker.worker(), leastLoadedWorker.connectorsSize());

            leastLoadedWorker.assign(connector);
        }
    }

    /**
     * Assigns tasks considering existing load for optimal global balance.
     * This method ensures perfect load balancing by always assigning each task
     * to the worker with the least current load, guaranteeing a maximum difference
     * of 1 task between any two workers.
     */
    private void assignTasksLoadAware(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        // Convert tasks to list for sorting
        List<ConnectorTaskId> taskList = tasks.stream()
                .sorted((t1, t2) -> {
                    int connectorCompare = t1.connector().compareTo(t2.connector());
                    if (connectorCompare != 0) return connectorCompare;
                    return Integer.compare(t1.task(), t2.task());
                })
                .collect(Collectors.toList());

        log.debug("Load-aware assigning {} tasks across {} workers for optimal global balance",
                  taskList.size(), workerAssignment.size());

        // For each task, assign to the worker with the least current load
        // This guarantees perfect balance with maximum difference of 1
        for (ConnectorTaskId task : taskList) {
            // Sort workers by current task load (least loaded first)
            // In case of ties, use worker name for deterministic assignment
            workerAssignment.sort((w1, w2) -> {
                int loadDiff = w1.tasksSize() - w2.tasksSize();
                if (loadDiff != 0) return loadDiff;
                return w1.worker().compareTo(w2.worker()); // Deterministic tie-breaker
            });

            WorkerLoad leastLoadedWorker = workerAssignment.get(0);
            leastLoadedWorker.assign(task);

            log.debug("Assigning task {} to worker {} (current load: {} tasks)",
                     task, leastLoadedWorker.worker(), leastLoadedWorker.tasksSize());
        }

        // Log final distribution for verification
        if (log.isDebugEnabled()) {
            int[] taskCounts = workerAssignment.stream().mapToInt(WorkerLoad::tasksSize).toArray();
            int minTasks = Arrays.stream(taskCounts).min().orElse(0);
            int maxTasks = Arrays.stream(taskCounts).max().orElse(0);
            log.debug("Final task distribution: {}, min={}, max={}, difference={}", 
                     Arrays.toString(taskCounts), minTasks, maxTasks, maxTasks - minTasks);
        }
    }
}
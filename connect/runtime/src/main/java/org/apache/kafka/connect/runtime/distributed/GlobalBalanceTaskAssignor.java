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
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.kafka.connect.runtime.ConnectorConfig;

/**
 * An assignor that extends the IncrementalCooperativeAssignor to provide global balance task assignment.
 *
 * This assignor ensures strict balance requirements:
 * 1. Per-Consumer Balance: Task count difference across workers ≤ 1 for each consumer group
 * 2. Global Balance: Total task count difference across workers ≤ 1
 *
 * The assignor handles four main scenarios:
 * 1. Initial allocation (from unassigned state)
 * 2. Configuration changes (maintains affinity)
 * 3. Worker scale down (redistributes from removed workers)
 * 4. Worker scale up (rebalances high-task consumers)
 *
 * @see <a href="https://github.com/triplelift-internal/kafka/blob/3.6.2-tl/GLOBAL_BALANCE_TASK_ASSIGNOR_BEHAVIOR.md">
 * Global Balance Task Assignor Behavior Documentation</a>
 */
public class GlobalBalanceTaskAssignor extends IncrementalCooperativeAssignor {
    private final Logger log;

    // For testing
    ClusterConfigState configSnapshot;

    public GlobalBalanceTaskAssignor(LogContext logContext, Time time, int maxDelay) {
        super(logContext, time, maxDelay);
        this.log = logContext.logger(GlobalBalanceTaskAssignor.class);
    }

    @Override
    protected void assignConnectors(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        if (connectors.isEmpty()) {
            return;
        }

        log.debug("Globally balancing {} connectors across {} workers", connectors.size(), workerAssignment.size());

        // Group connectors by their consumer group (extract from connector name)
        Map<String, List<String>> connectorsByConsumer = groupConnectorsByConsumer(connectors);

        // Sort consumers by connector count (descending) to prioritize high-count consumers
        List<String> sortedConsumers = connectorsByConsumer.keySet().stream()
                .sorted(Comparator.comparingInt((String consumer) -> connectorsByConsumer.get(consumer).size()).reversed())
                .collect(Collectors.toList());

        // Apply global balance assignment for each consumer group
        for (String consumer : sortedConsumers) {
            List<String> consumerConnectors = connectorsByConsumer.get(consumer);
            assignConnectorsWithGlobalBalance(workerAssignment, consumerConnectors);
        }
    }

    @Override
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        log.debug("Globally balancing {} tasks across {} workers", tasks.size(), workerAssignment.size());

        // Group tasks by their consumer group (extract from connector name)
        Map<String, List<ConnectorTaskId>> tasksByConsumer = groupTasksByConsumer(tasks);

        // Sort consumers by task count (descending) to prioritize high-count consumers
        List<String> sortedConsumers = tasksByConsumer.keySet().stream()
                .sorted(Comparator.comparingInt((String consumer) -> tasksByConsumer.get(consumer).size()).reversed())
                .collect(Collectors.toList());

        // Apply global balance assignment for each consumer group
        for (String consumer : sortedConsumers) {
            List<ConnectorTaskId> consumerTasks = tasksByConsumer.get(consumer);
            assignTasksWithGlobalBalance(workerAssignment, consumerTasks);
        }
    }

    /**
     * Groups connectors by their consumer group identifier.
     * Assumes connector names follow pattern: consumer-connector-name
     */
    private Map<String, List<String>> groupConnectorsByConsumer(Collection<String> connectors) {
        Map<String, List<String>> result = new TreeMap<>();

        for (String connector : connectors) {
            String consumer = extractConsumerFromConnector(connector);
            result.computeIfAbsent(consumer, k -> new ArrayList<>()).add(connector);
        }

        return result;
    }

    /**
     * Groups tasks by their consumer group identifier.
     * Assumes connector names follow pattern: consumer-connector-name
     */
    private Map<String, List<ConnectorTaskId>> groupTasksByConsumer(Collection<ConnectorTaskId> tasks) {
        Map<String, List<ConnectorTaskId>> result = new TreeMap<>();

        for (ConnectorTaskId task : tasks) {
            String consumer = extractConsumerFromConnector(task.connector());
            result.computeIfAbsent(consumer, k -> new ArrayList<>()).add(task);
        }

        return result;
    }

    /**
     * Extracts consumer group identifier from connector configuration.
     * Uses the connector's "name" config value with "connect-" prefix.
     * e.g., connector with name "s3-sink-ctv_usage" -> "connect-s3-sink-ctv_usage"
     */
    protected String extractConsumerFromConnector(String connectorName) {
        if (configSnapshot != null) {
            Map<String, String> connectorConfig = configSnapshot.connectorConfig(connectorName);
            if (connectorConfig != null) {
                String name = connectorConfig.get("name");
                if (name != null && !name.isEmpty()) {
                    return "connect-" + name;
                }
            }
        }

        // Fallback: use connector name directly with prefix if config is not available
        return "connect-" + connectorName;
    }

    /**
     * Gets the maximum number of tasks configured for a connector.
     * Defaults to 1 if not specified or configuration is not available.
     * Always uses the latest configSnapshot to get the current config.
     */
    protected int getTasksMaxForConnector(String connectorName) {
        if (configSnapshot != null) {
            Map<String, String> connectorConfig = configSnapshot.connectorConfig(connectorName);
            if (connectorConfig != null) {
                String tasksMaxStr = connectorConfig.get("tasks.max");
                if (tasksMaxStr != null && !tasksMaxStr.isEmpty()) {
                    try {
                        return Integer.parseInt(tasksMaxStr);
                    } catch (NumberFormatException e) {
                        log.warn("Invalid tasks.max value '{}' for connector '{}', defaulting to 1",
                                tasksMaxStr, connectorName);
                    }
                }
            }
        }
        return ConnectorConfig.TASKS_MAX_DEFAULT;
    }

    /**
     * Assigns connectors for a single consumer group ensuring per-consumer balance.
     */
    private void assignConnectorsWithGlobalBalance(List<WorkerLoad> workerAssignment, List<String> connectors) {
        if (connectors.isEmpty()) {
            return;
        }

        String consumerGroup = extractConsumerFromConnector(connectors.get(0));
        log.debug("Assigning {} connectors for consumer group '{}': {}",
                connectors.size(), consumerGroup, connectors);

        // Log tasks.max configuration for each connector
        for (String connector : connectors) {
            int tasksMax = getTasksMaxForConnector(connector);
            log.debug("Connector '{}' configured with tasks.max={}", connector, tasksMax);
        }

        // Sort workers by current connector load (ascending)
        workerAssignment.sort(WorkerLoad.connectorComparator());

        // Track current connector assignments per worker for this consumer
        Map<String, Integer> currentConnectorCount = new HashMap<>();
        for (WorkerLoad worker : workerAssignment) {
            long count = worker.connectors().stream()
                    .filter(c -> extractConsumerFromConnector(c).equals(consumerGroup))
                    .count();
            currentConnectorCount.put(worker.worker(), (int) count);
        }

        // Assign connectors round-robin style with balance constraints
        for (String connector : connectors) {
            // Find the worker with minimum connectors for this consumer group
            WorkerLoad targetWorker = workerAssignment.stream()
                    .min(Comparator.comparingInt(w -> currentConnectorCount.get(w.worker())))
                    .orElse(workerAssignment.get(0));

            log.debug("Assigning connector {} to worker {}", connector, targetWorker.worker());
            targetWorker.assign(connector);
            currentConnectorCount.put(targetWorker.worker(),
                    currentConnectorCount.get(targetWorker.worker()) + 1);
        }
    }

    /**
     * Assigns tasks for a single consumer group ensuring per-consumer balance.
     */
    private void assignTasksWithGlobalBalance(List<WorkerLoad> workerAssignment, List<ConnectorTaskId> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        String consumerGroup = extractConsumerFromConnector(tasks.get(0).connector());
        log.debug("Assigning {} tasks for consumer group '{}': {}",
                tasks.size(), consumerGroup, tasks);

        // Sort workers by current task load (ascending)
        workerAssignment.sort(WorkerLoad.taskComparator());

        // Track current task assignments per worker for this consumer
        Map<String, Integer> currentTaskCount = new HashMap<>();
        for (WorkerLoad worker : workerAssignment) {
            long count = worker.tasks().stream()
                    .filter(t -> extractConsumerFromConnector(t.connector()).equals(consumerGroup))
                    .count();
            currentTaskCount.put(worker.worker(), (int) count);
        }

        // Assign tasks round-robin style with balance constraints
        for (ConnectorTaskId task : tasks) {
            // Find the worker with minimum tasks for this consumer group
            WorkerLoad targetWorker = workerAssignment.stream()
                    .min(Comparator.comparingInt(w -> currentTaskCount.get(w.worker())))
                    .orElse(workerAssignment.get(0));

            log.debug("Assigning task {} to worker {}", task, targetWorker.worker());
            targetWorker.assign(task);
            currentTaskCount.put(targetWorker.worker(),
                    currentTaskCount.get(targetWorker.worker()) + 1);
        }
    }

    @Override
    protected ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        log.info("Performing global balance task assignment for generation {}", currentGenerationId);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Performing global balance task assignment for generation {}", currentGenerationId);

        // Store the latest config snapshot for use by all methods during this assignment round
        this.configSnapshot = configSnapshot;

        // Step 1: Handle tasks.max configuration changes FIRST
        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        TaskConfigChangeResult configChanges = detectTaskConfigChanges(configuredConnectors, memberAssignments);
        TaskConfigUpdateResult updateResult = handleTaskConfigChanges(configChanges, memberAssignments);

        // Use updated assignments after handling config changes
        Map<String, ConnectorsAndTasks> workingAssignments = updateResult.updatedAssignments;

        // Step 2: Build the final configured task set (including new tasks from tasks.max increases)
        Set<ConnectorTaskId> baseConfiguredTasks = configuredConnectors.stream()
                .flatMap(connector -> configSnapshot.tasks(connector).stream())
                .collect(Collectors.toSet());

        // Add newly created tasks from tasks.max increases
        Set<ConnectorTaskId> allConfiguredTasks = new HashSet<>(baseConfiguredTasks);
        allConfiguredTasks.addAll(updateResult.createdTasks);

        log.debug("Total configured connectors: {}, Base configured tasks: {}, Tasks from config changes: {}, Total tasks: {}",
                configuredConnectors.size(), baseConfiguredTasks.size(), updateResult.createdTasks.size(), allConfiguredTasks.size());

        // Step 3: Determine scenario type (using updated assignments)
        GlobalBalanceTaskAssignorScenarioType balanceScenarioType = getTypeOfTaskBalanceScenario(
                workingAssignments, configuredConnectors, allConfiguredTasks);
        log.info("Detected balance scenario type: {}", balanceScenarioType);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Detected balance scenario type: {}", balanceScenarioType);

        // Step 4: Create worker loads from working assignments
        List<WorkerLoad> workerLoads = workingAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());
        log.debug("Created {} worker loads for assignment", workerLoads.size());

        // Step 5: Perform scenario-specific assignment with updated task sets
        ClusterAssignment result;
        if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.INITIAL_CONNECTOR_ALLOCATION) {
            result = handleInitialConnectorAllocation(workingAssignments, workerLoads, configuredConnectors, allConfiguredTasks);
        } else if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_DOWN ||
                   balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_CHANGED) {
            result = handleWorkerScaleDownOrChanged(configSnapshot, workingAssignments, configuredConnectors, allConfiguredTasks);
        } else if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_UP) {
            result = handleWorkerScaleUp(configSnapshot, lastCompletedGenerationId, currentGenerationId, workingAssignments, configuredConnectors, allConfiguredTasks);
        } else {
            result = handleOtherScenario(configSnapshot, lastCompletedGenerationId, currentGenerationId, workingAssignments, configuredConnectors, allConfiguredTasks);
        }

        // Step 6: Merge the config change revocations with scenario results
        return mergeConfigChangeRevocations(result, updateResult.removedTasks, configChanges.connectorsWithChanges);
    }

    /**
     * Detects connectors with tasks.max configuration changes and returns the expected task sets.
     * This method compares the current tasks.max values with the actual running task counts.
     */
    private TaskConfigChangeResult detectTaskConfigChanges(
            Set<String> configuredConnectors,
            Map<String, ConnectorsAndTasks> memberAssignments) {

        Set<String> connectorsWithChanges = new HashSet<>();
        Set<ConnectorTaskId> tasksToCreate = new HashSet<>();
        Set<ConnectorTaskId> tasksToRemove = new HashSet<>();

        // Get currently active assignments across all workers
        Set<ConnectorTaskId> currentlyActiveTasks = memberAssignments.values().stream()
                .flatMap(assignment -> assignment.tasks().stream())
                .collect(Collectors.toSet());

        for (String connector : configuredConnectors) {
            // Get the expected task count from tasks.max configuration
            int expectedTaskCount = getTasksMaxForConnector(connector);

            // Get currently active tasks for this connector
            Set<ConnectorTaskId> currentTasks = currentlyActiveTasks.stream()
                    .filter(task -> task.connector().equals(connector))
                    .collect(Collectors.toSet());

            int currentTaskCount = currentTasks.size();

            if (expectedTaskCount != currentTaskCount) {
                log.info("Connector '{}' tasks.max changed: current={}, expected={}",
                        connector, currentTaskCount, expectedTaskCount);
                connectorsWithChanges.add(connector);

                if (expectedTaskCount > currentTaskCount) {
                    // Need to create new tasks
                    for (int i = currentTaskCount; i < expectedTaskCount; i++) {
                        tasksToCreate.add(new ConnectorTaskId(connector, i));
                    }
                    log.debug("Connector '{}' needs {} new tasks: {}",
                            connector, expectedTaskCount - currentTaskCount,
                            tasksToCreate.stream().filter(t -> t.connector().equals(connector)).collect(Collectors.toList()));
                } else {
                    // Need to remove excess tasks
                    List<ConnectorTaskId> sortedCurrentTasks = currentTasks.stream()
                            .sorted(Comparator.comparingInt(ConnectorTaskId::task))
                            .collect(Collectors.toList());

                    for (int i = expectedTaskCount; i < currentTaskCount; i++) {
                        if (i < sortedCurrentTasks.size()) {
                            tasksToRemove.add(sortedCurrentTasks.get(i));
                        }
                    }
                    log.debug("Connector '{}' needs {} tasks removed: {}",
                            connector, currentTaskCount - expectedTaskCount,
                            tasksToRemove.stream().filter(t -> t.connector().equals(connector)).collect(Collectors.toList()));
                }
            }
        }

        return new TaskConfigChangeResult(connectorsWithChanges, tasksToCreate, tasksToRemove);
    }

    /**
     * Handles tasks.max configuration changes by creating/removing tasks as needed.
     * Returns updated memberAssignments and the tasks that were created/removed.
     */
    private TaskConfigUpdateResult handleTaskConfigChanges(
            TaskConfigChangeResult configChanges,
            Map<String, ConnectorsAndTasks> memberAssignments) {

        if (configChanges.connectorsWithChanges.isEmpty()) {
            return new TaskConfigUpdateResult(memberAssignments, Collections.emptySet(), Collections.emptySet());
        }

        // Create a mutable copy of member assignments
        Map<String, ConnectorsAndTasks> updatedAssignments = new HashMap<>();
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            updatedAssignments.put(entry.getKey(), new ConnectorsAndTasks.Builder()
                    .with(entry.getValue().connectors(), entry.getValue().tasks())
                    .build());
        }

        Set<ConnectorTaskId> actuallyRemovedTasks = new HashSet<>();

        // Remove excess tasks from current assignments
        for (ConnectorTaskId taskToRemove : configChanges.tasksToRemove) {
            for (Map.Entry<String, ConnectorsAndTasks> entry : updatedAssignments.entrySet()) {
                if (entry.getValue().tasks().contains(taskToRemove)) {
                    entry.getValue().tasks().remove(taskToRemove);
                    actuallyRemovedTasks.add(taskToRemove);
                    log.debug("Removed task {} from worker {}", taskToRemove, entry.getKey());
                    break;
                }
            }
        }

        // Note: New tasks in tasksToCreate will be assigned through the normal assignment process
        // They don't need to be added to assignments here since they'll be treated as "newly configured"

        return new TaskConfigUpdateResult(updatedAssignments, configChanges.tasksToCreate, actuallyRemovedTasks);
    }

    /**
     * Merges configuration change revocations with the cluster assignment result.
     */
    private ClusterAssignment mergeConfigChangeRevocations(
            ClusterAssignment scenarioResult,
            Set<ConnectorTaskId> configRemovedTasks,
            Set<String> connectorsWithConfigChanges) {

        if (configRemovedTasks.isEmpty()) {
            return scenarioResult;
        }

        // Create merged revocation maps
        Map<String, Collection<ConnectorTaskId>> mergedRevokedTasks = new HashMap<>(scenarioResult.newlyRevokedTasks());
        Map<String, Collection<String>> mergedRevokedConnectors = new HashMap<>(scenarioResult.newlyRevokedConnectors());

        // Add config-removed tasks to revocations (though they may not have explicit worker assignments)
        // This is mainly for tracking/logging purposes
        log.info("Merging {} tasks removed due to tasks.max decreases", configRemovedTasks.size());

        // Note: The removed tasks have already been removed from assignments in handleTaskConfigChanges()
        // This merge is primarily for completeness in the ClusterAssignment result

        return new ClusterAssignment(
                scenarioResult.newlyAssignedConnectors(),
                scenarioResult.newlyAssignedTasks(),
                mergedRevokedConnectors,
                mergedRevokedTasks,
                scenarioResult.allAssignedConnectors(),
                scenarioResult.allAssignedTasks()
        );
    }

    // Helper classes for task configuration changes
    private static class TaskConfigChangeResult {
        final Set<String> connectorsWithChanges;
        final Set<ConnectorTaskId> tasksToCreate;
        final Set<ConnectorTaskId> tasksToRemove;

        TaskConfigChangeResult(Set<String> connectorsWithChanges, Set<ConnectorTaskId> tasksToCreate, Set<ConnectorTaskId> tasksToRemove) {
            this.connectorsWithChanges = connectorsWithChanges;
            this.tasksToCreate = tasksToCreate;
            this.tasksToRemove = tasksToRemove;
        }
    }

    private static class TaskConfigUpdateResult {
        final Map<String, ConnectorsAndTasks> updatedAssignments;
        final Set<ConnectorTaskId> createdTasks;
        final Set<ConnectorTaskId> removedTasks;

        TaskConfigUpdateResult(Map<String, ConnectorsAndTasks> updatedAssignments,
                              Set<ConnectorTaskId> createdTasks,
                              Set<ConnectorTaskId> removedTasks) {
            this.updatedAssignments = updatedAssignments;
            this.createdTasks = createdTasks;
            this.removedTasks = removedTasks;
        }
    }

    /**
     * Handles initial connector allocation scenario.
     * This scenario occurs when new connectors/tasks are being deployed to the cluster
     * and need to be assigned for the first time, ensuring global balance requirements.
     */
    private ClusterAssignment handleInitialConnectorAllocation(
            Map<String, ConnectorsAndTasks> memberAssignments,
            List<WorkerLoad> workerLoads,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {

        log.info("Handling initial connector allocation scenario");
        log.error("INFO GlobalBalanceTaskAssignor: Handling initial connector allocation scenario");

        // Determine if this is a completely empty cluster or new consumer groups being added
        boolean isCompletelyEmptyCluster = memberAssignments.values().stream()
                .allMatch(assignment -> assignment.connectors().isEmpty() && assignment.tasks().isEmpty());

        if (isCompletelyEmptyCluster) {
            log.debug("Completely empty cluster - clearing all assignments for fresh start");
            for (WorkerLoad worker : workerLoads) {
                worker.connectors().clear();
                worker.tasks().clear();
            }
        }

        // New consumer groups, assign the new (unassigned) connectors and tasks
        Set<String> currentlyAssignedConnectors = memberAssignments.values().stream()
                .flatMap(assignment -> assignment.connectors().stream())
                .collect(Collectors.toSet());

        Set<ConnectorTaskId> currentlyAssignedTasks = memberAssignments.values().stream()
                .flatMap(assignment -> assignment.tasks().stream())
                .collect(Collectors.toSet());

        Set<String> newConnectors = configuredConnectors.stream()
                .filter(connector -> !currentlyAssignedConnectors.contains(connector))
                .collect(Collectors.toSet());

        Set<ConnectorTaskId> newTasks = configuredTasks.stream()
                .filter(task -> !currentlyAssignedTasks.contains(task))
                .collect(Collectors.toSet());

        log.debug("Assigning {} new connectors and {} new tasks using Scenario 1 logic",
                newConnectors.size(), newTasks.size());

        assignConnectors(workerLoads, newConnectors);
        assignTasks(workerLoads, newTasks);

        // Build the cluster assignment from balanced allocation
        Map<String, Collection<String>> allConnectorAssignments = workerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));

        Map<String, Collection<ConnectorTaskId>> allTaskAssignments = workerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::tasks
                ));

        // Calculate newly assigned connectors and tasks (what was just assigned in this round)
        Map<String, Collection<String>> newlyAssignedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks = new HashMap<>();

        for (WorkerLoad worker : workerLoads) {
            String workerId = worker.worker();

            // Find newly assigned connectors for this worker
            Collection<String> workerNewConnectors = worker.connectors().stream()
                    .filter(newConnectors::contains)
                    .collect(Collectors.toList());
            if (!workerNewConnectors.isEmpty()) {
                newlyAssignedConnectors.put(workerId, workerNewConnectors);
            }

            // Find newly assigned tasks for this worker
            Collection<ConnectorTaskId> workerNewTasks = worker.tasks().stream()
                    .filter(newTasks::contains)
                    .collect(Collectors.toList());
            if (!workerNewTasks.isEmpty()) {
                newlyAssignedTasks.put(workerId, workerNewTasks);
            }
        }

        log.debug("Global balance assignment complete - newly assigned connectors: {}, newly assigned tasks: {}",
                newlyAssignedConnectors, newlyAssignedTasks);
        log.debug("Total assignments after balance - all connectors: {}, all tasks: {}",
                allConnectorAssignments, allTaskAssignments);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Global balance assignment complete - newly assigned connectors: {},"
                + " newly assigned tasks: {}. Total assignments after balance - all connectors: {}, all tasks: {}",
                newlyAssignedConnectors, newlyAssignedTasks, allConnectorAssignments, allTaskAssignments);

        // Update previousMembers for next rebalance cycle
        previousMembers = memberAssignments.keySet();

        // Return the globally balanced cluster assignment for Scenario 1
        return new ClusterAssignment(
                newlyAssignedConnectors,  // newly assigned connectors (only the new ones)
                newlyAssignedTasks,       // newly assigned tasks (only the new ones)
                Collections.emptyMap(),   // no revocations in initial assignment
                Collections.emptyMap(),   // no revocations in initial assignment
                allConnectorAssignments,  // all assigned connectors after this rebalance
                allTaskAssignments        // all assigned tasks after this rebalance
        );
    }

    /**
     * Handles worker scale down and worker changed scenarios.
     * Both scenarios follow the same logic: redistribute only unassigned work from removed workers
     * while preserving existing assignments on remaining workers.
     */
    private ClusterAssignment handleWorkerScaleDownOrChanged(
            ClusterConfigState configSnapshot,
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {

        log.info("Handling worker scale down or changed scenario");
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Handling worker scale down or changed scenario.");

        Set<String> currentMembers = memberAssignments.keySet();
        Set<String> removedWorkers = previousMembers.stream()
                .filter(worker -> !currentMembers.contains(worker))
                .collect(Collectors.toSet());

        log.info("Detected {} removed workers: {}", removedWorkers.size(), removedWorkers);

        // Find unassigned connectors and tasks from removed workers
        Set<String> unassignedConnectors = configuredConnectors.stream()
                .filter(connector -> {
                    // Check if this connector is currently assigned to any remaining worker
                    return memberAssignments.values().stream()
                            .noneMatch(assignment -> assignment.connectors().contains(connector));
                })
                .collect(Collectors.toSet());

        Set<ConnectorTaskId> unassignedTasks = configuredTasks.stream()
                .filter(task -> {
                    // Check if this task is currently assigned to any remaining worker
                    return memberAssignments.values().stream()
                            .noneMatch(assignment -> assignment.tasks().contains(task));
                })
                .collect(Collectors.toSet());

        log.info("Found {} unassigned connectors and {} unassigned tasks from removed workers",
                unassignedConnectors.size(), unassignedTasks.size());

        // If there are no unassigned connectors/tasks, no rebalancing needed
        if (unassignedConnectors.isEmpty() && unassignedTasks.isEmpty()) {
            log.info("No unassigned work found. Current assignments are preserved.");

            // Build assignments from current state (no changes)
            Map<String, Collection<String>> currentConnectorAssignments = memberAssignments.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new ArrayList<>(entry.getValue().connectors())
                    ));

            Map<String, Collection<ConnectorTaskId>> currentTaskAssignments = memberAssignments.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new ArrayList<>(entry.getValue().tasks())
                    ));

            // Update previousMembers for next rebalance cycle
            previousMembers = currentMembers;

            // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
            // Setting it to info or warn produces too much data to track rebalancing activity
            log.error("INFO GlobalBalanceTaskAssignor: There are no unassigned connectors/tasks, no rebalancing needed.");

            return new ClusterAssignment(
                    Collections.emptyMap(),       // no newly assigned connectors
                    Collections.emptyMap(),       // no newly assigned tasks
                    Collections.emptyMap(),       // no revocations needed
                    Collections.emptyMap(),       // no revocations needed
                    currentConnectorAssignments,  // all current assignments
                    currentTaskAssignments        // all current assignments
            );
        }

        // Create WorkerLoad objects for remaining workers (preserving existing assignments)
        List<WorkerLoad> remainingWorkerLoads = memberAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());

        log.debug("Remaining workers with preserved assignments: {}",
                remainingWorkerLoads.stream()
                        .collect(Collectors.toMap(
                                WorkerLoad::worker,
                                w -> "connectors=" + w.connectors().size() + ", tasks=" + w.tasks().size()
                        )));

        // Assign only the unassigned connectors and tasks using global balance
        assignConnectors(remainingWorkerLoads, unassignedConnectors);
        assignTasks(remainingWorkerLoads, unassignedTasks);

        // Build the cluster assignment
        Map<String, Collection<String>> finalConnectorAssignments = remainingWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));

        Map<String, Collection<ConnectorTaskId>> finalTaskAssignments = remainingWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::tasks
                ));

        // Calculate newly assigned connectors and tasks (only the unassigned ones)
        Map<String, Collection<String>> scaleDownNewConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> scaleDownNewTasks = new HashMap<>();

        for (WorkerLoad worker : remainingWorkerLoads) {
            String workerId = worker.worker();

            // Find newly assigned connectors for this worker
            Collection<String> workerNewConnectors = worker.connectors().stream()
                    .filter(unassignedConnectors::contains)
                    .collect(Collectors.toList());
            if (!workerNewConnectors.isEmpty()) {
                scaleDownNewConnectors.put(workerId, workerNewConnectors);
            }

            // Find newly assigned tasks for this worker
            Collection<ConnectorTaskId> workerNewTasks = worker.tasks().stream()
                    .filter(unassignedTasks::contains)
                    .collect(Collectors.toList());
            if (!workerNewTasks.isEmpty()) {
                scaleDownNewTasks.put(workerId, workerNewTasks);
            }
        }

        log.debug("Scale down rebalance complete - newly assigned connectors: {}, newly assigned tasks: {}",
                scaleDownNewConnectors, scaleDownNewTasks);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Scale down rebalance complete - newly assigned connectors: {}, newly assigned tasks: {}",
                scaleDownNewConnectors, scaleDownNewTasks);

        // Update previousMembers for next rebalance cycle
        previousMembers = currentMembers;

        // Return the cluster assignment for worker scale down
        return new ClusterAssignment(
                scaleDownNewConnectors,   // newly assigned connectors (only from removed workers)
                scaleDownNewTasks,        // newly assigned tasks (only from removed workers)
                Collections.emptyMap(),   // no revocations needed (workers already gone)
                Collections.emptyMap(),   // no revocations needed (workers already gone)
                finalConnectorAssignments,  // all assigned connectors after rebalance
                finalTaskAssignments        // all assigned tasks after rebalance
        );
    }

    /**
     * Handles worker scale up scenario.
     * This scenario occurs when new workers are added to the cluster and existing
     * assignments need to be rebalanced to utilize the new capacity while maintaining
     * strict balance requirements.
     */
    private ClusterAssignment handleWorkerScaleUp(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {

        log.info("Handling worker scale up scenario");
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Handling worker scale up scenario");

        Set<String> currentMembers = memberAssignments.keySet();
        Set<String> addedWorkers = currentMembers.stream()
                .filter(worker -> !previousMembers.contains(worker))
                .collect(Collectors.toSet());

        log.info("Detected {} added workers: {}", addedWorkers.size(), addedWorkers);

        // Create WorkerLoad objects from current assignments
        List<WorkerLoad> allWorkerLoads = memberAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());

        int totalWorkers = allWorkerLoads.size();
        log.debug("Total workers after scale up: {}", totalWorkers);

        // Group current connectors and tasks by consumer
        Map<String, List<String>> connectorsByConsumer = allWorkerLoads.stream()
                .flatMap(worker -> worker.connectors().stream())
                .collect(Collectors.groupingBy(this::extractConsumerFromConnector));

        Map<String, List<ConnectorTaskId>> tasksByConsumer = allWorkerLoads.stream()
                .flatMap(worker -> worker.tasks().stream())
                .collect(Collectors.groupingBy(task -> extractConsumerFromConnector(task.connector())));

        // Sort consumers by task count (descending) to prioritize highest task count consumers
        List<String> sortedConsumers = tasksByConsumer.keySet().stream()
                .sorted((consumer1, consumer2) -> {
                    int tasks1 = tasksByConsumer.get(consumer1).size();
                    int tasks2 = tasksByConsumer.get(consumer2).size();
                    if (tasks1 != tasks2) {
                        return Integer.compare(tasks2, tasks1); // Descending by task count
                    }
                    // If task counts are equal, sort by tasks.max (descending)
                    int tasksMax1 = getMaxTasksForConsumerGroup(consumer1, connectorsByConsumer);
                    int tasksMax2 = getMaxTasksForConsumerGroup(consumer2, connectorsByConsumer);
                    return Integer.compare(tasksMax2, tasksMax1);
                })
                .collect(Collectors.toList());

        // Track which tasks and connectors will be revoked and reassigned
        Map<String, Collection<String>> revokedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> revokedTasks = new HashMap<>();
        Set<String> connectorsToRebalance = new HashSet<>();
        Set<ConnectorTaskId> tasksToRebalance = new HashSet<>();

        // Calculate current global balance to determine if rebalancing is needed
        int totalTasks = allWorkerLoads.stream().mapToInt(worker -> worker.tasks().size()).sum();
        int idealTasksPerWorker = totalTasks / totalWorkers;

        // Check if global balance is violated (max - min > 1)
        int minLoad = allWorkerLoads.stream().mapToInt(worker -> worker.tasks().size()).min().orElse(0);
        int maxLoad = allWorkerLoads.stream().mapToInt(worker -> worker.tasks().size()).max().orElse(0);

        boolean needsGlobalRebalancing = (maxLoad - minLoad) > 1;

        if (!needsGlobalRebalancing) {
            log.debug("Global balance already achieved (max:{}, min:{}), no rebalancing needed", maxLoad, minLoad);
        } else {
            log.debug("Global balance violated (max:{}, min:{}), rebalancing needed", maxLoad, minLoad);

            // When global balance is violated, we need to move tasks from overloaded to underloaded workers
            // Collect all tasks from overloaded workers for redistribution
            List<WorkerLoad> sortedWorkers = allWorkerLoads.stream()
                    .sorted((w1, w2) -> Integer.compare(w2.tasks().size(), w1.tasks().size()))
                    .collect(Collectors.toList());

            for (WorkerLoad worker : sortedWorkers) {
                int currentTasks = worker.tasks().size();
                int targetTasks = idealTasksPerWorker + (totalTasks % totalWorkers > 0 ? 1 : 0);
                int remainingExtra = totalTasks % totalWorkers;
                if (remainingExtra > 0) remainingExtra--;

                if (currentTasks > targetTasks) {
                    // Revoke excess tasks for redistribution
                    List<ConnectorTaskId> excessTasks = new ArrayList<>(worker.tasks());
                    List<ConnectorTaskId> tasksToRevoke = excessTasks.subList(targetTasks, excessTasks.size());

                    revokedTasks.computeIfAbsent(worker.worker(), k -> new ArrayList<>()).addAll(tasksToRevoke);
                    tasksToRebalance.addAll(tasksToRevoke);

                    // Remove excess tasks from worker
                    worker.tasks().removeAll(tasksToRevoke);

                    // Also revoke corresponding connectors
                    List<String> connectorsToRevoke = tasksToRevoke.stream()
                            .map(task -> task.connector())
                            .distinct()
                            .collect(Collectors.toList());

                    revokedConnectors.computeIfAbsent(worker.worker(), k -> new ArrayList<>()).addAll(connectorsToRevoke);
                    connectorsToRebalance.addAll(connectorsToRevoke);
                    worker.connectors().removeAll(connectorsToRevoke);
                }
            }
        }

        // Only rebalance consumer groups with task count > number of workers (original logic for non-global cases)
        for (String consumer : sortedConsumers) {
            List<ConnectorTaskId> consumerTasks = tasksByConsumer.get(consumer);
            List<String> consumerConnectors = connectorsByConsumer.getOrDefault(consumer, Collections.emptyList());

            if (consumerTasks.size() > totalWorkers) {
                log.debug("Consumer '{}' has {} tasks > {} workers, triggering rebalance",
                        consumer, consumerTasks.size(), totalWorkers);

                // Calculate current distribution for this consumer
                Map<String, List<ConnectorTaskId>> currentTaskDistribution = new HashMap<>();
                Map<String, List<String>> currentConnectorDistribution = new HashMap<>();

                for (WorkerLoad worker : allWorkerLoads) {
                    currentTaskDistribution.put(worker.worker(), new ArrayList<>());
                    currentConnectorDistribution.put(worker.worker(), new ArrayList<>());
                }

                // Populate current distributions
                for (WorkerLoad worker : allWorkerLoads) {
                    worker.tasks().stream()
                            .filter(task -> extractConsumerFromConnector(task.connector()).equals(consumer))
                            .forEach(task -> currentTaskDistribution.get(worker.worker()).add(task));

                    worker.connectors().stream()
                            .filter(connector -> extractConsumerFromConnector(connector).equals(consumer))
                            .forEach(connector -> currentConnectorDistribution.get(worker.worker()).add(connector));
                }

                // Calculate ideal distribution
                int idealConsumerTasksPerWorker = consumerTasks.size() / totalWorkers;
                int extraTasks = consumerTasks.size() % totalWorkers;
                int idealConnectorsPerWorker = consumerConnectors.size() / totalWorkers;
                int extraConnectors = consumerConnectors.size() % totalWorkers;

                // Identify tasks and connectors to revoke for rebalancing
                for (Map.Entry<String, List<ConnectorTaskId>> entry : currentTaskDistribution.entrySet()) {
                    String workerId = entry.getKey();
                    List<ConnectorTaskId> workerTasks = entry.getValue();

                    int targetTaskCount = idealConsumerTasksPerWorker + (extraTasks > 0 ? 1 : 0);
                    if (extraTasks > 0) extraTasks--;

                    if (workerTasks.size() > targetTaskCount) {
                        List<ConnectorTaskId> tasksForRevocation = workerTasks.subList(targetTaskCount, workerTasks.size());

                        revokedTasks.computeIfAbsent(workerId, k -> new ArrayList<>()).addAll(tasksForRevocation);
                        tasksToRebalance.addAll(tasksForRevocation);

                        // Remove revoked tasks from worker
                        for (WorkerLoad worker : allWorkerLoads) {
                            if (worker.worker().equals(workerId)) {
                                tasksForRevocation.forEach(task -> worker.tasks().remove(task));
                                break;
                            }
                        }
                    }
                }

                // Reset extraConnectors for connector distribution
                extraConnectors = consumerConnectors.size() % totalWorkers;
                for (Map.Entry<String, List<String>> entry : currentConnectorDistribution.entrySet()) {
                    String workerId = entry.getKey();
                    List<String> workerConnectors = entry.getValue();

                    int targetConnectorCount = idealConnectorsPerWorker + (extraConnectors > 0 ? 1 : 0);
                    if (extraConnectors > 0) extraConnectors--;

                    if (workerConnectors.size() > targetConnectorCount) {
                        List<String> connectorsForRevocation = workerConnectors.subList(targetConnectorCount, workerConnectors.size());

                        revokedConnectors.computeIfAbsent(workerId, k -> new ArrayList<>()).addAll(connectorsForRevocation);
                        connectorsToRebalance.addAll(connectorsForRevocation);

                        // Remove revoked connectors from worker
                        for (WorkerLoad worker : allWorkerLoads) {
                            if (worker.worker().equals(workerId)) {
                                connectorsForRevocation.forEach(connector -> worker.connectors().remove(connector));
                                break;
                            }
                        }
                    }
                }
            }
        }

        log.debug("Rebalancing {} connectors and {} tasks across {} workers",
                connectorsToRebalance.size(), tasksToRebalance.size(), totalWorkers);

        // Assign the revoked connectors and tasks using global balance
        assignConnectors(allWorkerLoads, connectorsToRebalance);
        assignTasks(allWorkerLoads, tasksToRebalance);

        // Build final assignments
        Map<String, Collection<String>> finalConnectorAssignments = allWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));

        Map<String, Collection<ConnectorTaskId>> finalTaskAssignments = allWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::tasks
                ));

        // Calculate newly assigned connectors and tasks (what was just reassigned)
        Map<String, Collection<String>> newlyAssignedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks = new HashMap<>();

        for (WorkerLoad worker : allWorkerLoads) {
            String workerId = worker.worker();

            // Find newly assigned connectors for this worker
            Collection<String> workerNewConnectors = worker.connectors().stream()
                    .filter(connectorsToRebalance::contains)
                    .collect(Collectors.toList());
            if (!workerNewConnectors.isEmpty()) {
                newlyAssignedConnectors.put(workerId, workerNewConnectors);
            }

            // Find newly assigned tasks for this worker
            Collection<ConnectorTaskId> workerNewTasks = worker.tasks().stream()
                    .filter(tasksToRebalance::contains)
                    .collect(Collectors.toList());
            if (!workerNewTasks.isEmpty()) {
                newlyAssignedTasks.put(workerId, workerNewTasks);
            }
        }

        log.debug("Scale up rebalance complete - newly assigned connectors: {}, newly assigned tasks: {}",
                newlyAssignedConnectors, newlyAssignedTasks);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Scale up rebalance complete - newly assigned connectors: {}, newly assigned tasks: {}",
                newlyAssignedConnectors, newlyAssignedTasks);

        // Update previousMembers for next rebalance cycle
        previousMembers = currentMembers;

        // Return the cluster assignment for worker scale up
        return new ClusterAssignment(
                newlyAssignedConnectors,    // newly assigned connectors (rebalanced ones)
                newlyAssignedTasks,         // newly assigned tasks (rebalanced ones)
                revokedConnectors,          // revoked connectors for rebalancing
                revokedTasks,               // revoked tasks for rebalancing
                finalConnectorAssignments, // all assigned connectors after rebalance
                finalTaskAssignments       // all assigned tasks after rebalance
        );
    }

    /**
     * Handles other scenario including catch-all and configuration changes.
     * This scenario checks if tasks are already balanced across all workers, and
     * only triggers a rebalance if the current distribution violates balance requirements.
     * Per-Consumer Balance: Task count difference across workers ≤ 1 for each consumer group
     * Global Balance: Total task count difference across workers ≤ 1
     */
    private ClusterAssignment handleOtherScenario(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {

        log.info("Handling other scenario - checking for config changes and if current assignment is already balanced");
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Handling other scenario - checking for config changes and if current assignment is already balanced");

        Set<String> currentMembers = memberAssignments.keySet();

        // Step 1: Identify any configuration changes that need rebalancing
        // Get currently active assignments across all workers
        ConnectorsAndTasks activeAssignments = new ConnectorsAndTasks.Builder()
                .with(
                    memberAssignments.values().stream().flatMap(a -> a.connectors().stream()).collect(Collectors.toSet()),
                    memberAssignments.values().stream().flatMap(a -> a.tasks().stream()).collect(Collectors.toSet())
                ).build();

        // Find connectors with mismatched task counts (config changes)
        Set<String> connectorsNeedingRebalance = configuredConnectors.stream()
                .filter(connector -> {
                    int configuredTaskCount = configSnapshot.tasks(connector).size();
                    int activeTaskCount = (int) activeAssignments.tasks().stream()
                            .filter(task -> task.connector().equals(connector))
                            .count();

                    if (configuredTaskCount != activeTaskCount) {
                        log.debug("Connector {} needs rebalance: configured tasks = {}, active tasks = {}",
                                  connector, configuredTaskCount, activeTaskCount);
                        return true;
                    }
                    return false;
                })
                .collect(Collectors.toSet());

        // Step 2: Handle config changes if any were found
        if (!connectorsNeedingRebalance.isEmpty()) {
            log.info("Configuration changes detected for {} connectors: {}",
                    connectorsNeedingRebalance.size(), connectorsNeedingRebalance);

            // Get the tasks that need to be rebalanced
            Set<ConnectorTaskId> tasksToRebalance = configuredTasks.stream()
                    .filter(task -> connectorsNeedingRebalance.contains(task.connector()))
                    .collect(Collectors.toSet());

            return performRebalanceForConnectors(
                    memberAssignments,
                    currentMembers,
                    connectorsNeedingRebalance,
                    tasksToRebalance,
                    configuredTasks);
        }

        // Step 3: If no config changes, check balance requirements
        // Convert member assignments to worker loads for balance checking
        List<WorkerLoad> currentWorkerLoads = memberAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());

        // Check 1: Global balance - task count difference ≤ 1 across all workers
        int minGlobalLoad = currentWorkerLoads.stream().mapToInt(w -> w.tasks().size()).min().orElse(0);
        int maxGlobalLoad = currentWorkerLoads.stream().mapToInt(w -> w.tasks().size()).max().orElse(0);
        boolean globalBalanceViolated = (maxGlobalLoad - minGlobalLoad) > 1;

        log.debug("Global balance check: min={}, max={}, difference={}, balanced={}",
                minGlobalLoad, maxGlobalLoad, maxGlobalLoad - minGlobalLoad, !globalBalanceViolated);

        // Check 2: Per-consumer balance - task count difference ≤ 1 per consumer
        // Build task counts by consumer and worker
        Map<String, Map<String, Integer>> taskCountByConsumerAndWorker = buildTaskCountMap(currentWorkerLoads);

        // Find consumers with unbalanced task distribution
        Set<String> unbalancedConsumers = findUnbalancedConsumers(taskCountByConsumerAndWorker);
        boolean perConsumerBalanceViolated = !unbalancedConsumers.isEmpty();

        // Step 4: If both balance requirements are met, no rebalancing needed
        if (!globalBalanceViolated && !perConsumerBalanceViolated) {
            log.info("Current assignment is already balanced. No rebalancing needed.");

            // Return current assignment with no changes
            Map<String, Collection<String>> currentConnectorAssignments = memberAssignments.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new ArrayList<>(entry.getValue().connectors())
                    ));

            Map<String, Collection<ConnectorTaskId>> currentTaskAssignments = memberAssignments.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> new ArrayList<>(entry.getValue().tasks())
                    ));

            // Update previousMembers for next rebalance cycle
            previousMembers = currentMembers;

            // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
            // Setting it to info or warn produces too much data to track rebalancing activity
            log.error("INFO GlobalBalanceTaskAssignor: Balance requirements are met, no rebalancing needed");

            return new ClusterAssignment(
                    Collections.emptyMap(),       // no newly assigned connectors
                    Collections.emptyMap(),       // no newly assigned tasks
                    Collections.emptyMap(),       // no revocations needed
                    Collections.emptyMap(),       // no revocations needed
                    currentConnectorAssignments,  // current assignments preserved
                    currentTaskAssignments        // current assignments preserved
            );
        }

        // Step 5: Balance is violated, identify which consumers to rebalance
        log.info("Balance requirements violated. Performing selective rebalance.");
        log.debug("Global balance violated: {}, Per-consumer balance violated: {}",
                globalBalanceViolated, perConsumerBalanceViolated);

        // If no per-consumer violations but global balance violated,
        // we need to identify which consumers to move for load balancing
        if (globalBalanceViolated && unbalancedConsumers.isEmpty()) {
            unbalancedConsumers = identifyConsumersForGlobalRebalance(
                    currentWorkerLoads,
                    taskCountByConsumerAndWorker,
                    configuredConnectors);
        }

        // Create a final copy for use in lambda expressions
        final Set<String> finalUnbalancedConsumers = new HashSet<>(unbalancedConsumers);

        // Get all connectors and tasks for the unbalanced consumers
        Set<String> connectorsToRebalance = configuredConnectors.stream()
                .filter(connector -> finalUnbalancedConsumers.contains(extractConsumerFromConnector(connector)))
                .collect(Collectors.toSet());

        Set<ConnectorTaskId> tasksToRebalance = configuredTasks.stream()
                .filter(task -> finalUnbalancedConsumers.contains(extractConsumerFromConnector(task.connector())))
                .collect(Collectors.toSet());

        log.debug("Selective rebalance will reassign {} connectors and {} tasks from {} unbalanced consumers",
                connectorsToRebalance.size(), tasksToRebalance.size(), unbalancedConsumers.size());
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Selective rebalance will reassign {} connectors and {} tasks from {} unbalanced consumers",
                connectorsToRebalance.size(), tasksToRebalance.size(), unbalancedConsumers.size());

        return performRebalanceForConnectors(
                memberAssignments,
                currentMembers,
                connectorsToRebalance,
                tasksToRebalance,
                configuredTasks);
    }

    /**
     * Helper method to build a map of task counts by consumer and worker
     */
    private Map<String, Map<String, Integer>> buildTaskCountMap(List<WorkerLoad> workerLoads) {
        Map<String, Map<String, Integer>> taskCountByConsumerAndWorker = new HashMap<>();

        // Count tasks by consumer for each worker
        for (WorkerLoad worker : workerLoads) {
            String workerId = worker.worker();

            for (ConnectorTaskId task : worker.tasks()) {
                String consumer = extractConsumerFromConnector(task.connector());
                Map<String, Integer> workerCounts = taskCountByConsumerAndWorker
                        .computeIfAbsent(consumer, k -> new HashMap<>());
                workerCounts.put(workerId, workerCounts.getOrDefault(workerId, 0) + 1);
            }
        }

        // Ensure all workers have entries for all consumers (with count 0 if no tasks)
        for (Map<String, Integer> workerCounts : taskCountByConsumerAndWorker.values()) {
            for (WorkerLoad worker : workerLoads) {
                workerCounts.putIfAbsent(worker.worker(), 0);
            }
        }

        return taskCountByConsumerAndWorker;
    }

    /**
     * Helper method to find consumers with unbalanced task distribution
     */
    private Set<String> findUnbalancedConsumers(Map<String, Map<String, Integer>> taskCountByConsumerAndWorker) {
        Set<String> unbalancedConsumers = new HashSet<>();

        for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
            String consumer = entry.getKey();
            Map<String, Integer> workerCounts = entry.getValue();

            if (!workerCounts.isEmpty()) {
                int minCount = Collections.min(workerCounts.values());
                int maxCount = Collections.max(workerCounts.values());

                if ((maxCount - minCount) > 1) {
                    log.debug("Per-consumer balance violation for consumer {}: min={}, max={}, difference={}",
                            consumer, minCount, maxCount, maxCount - minCount);
                    unbalancedConsumers.add(consumer);
                }
            }
        }

        return unbalancedConsumers;
    }

    /**
     * Helper method to identify which consumers to rebalance for global balance
     */
    private Set<String> identifyConsumersForGlobalRebalance(
            List<WorkerLoad> workerLoads,
            Map<String, Map<String, Integer>> taskCountByConsumerAndWorker,
            Set<String> configuredConnectors) {

        log.debug("Only global balance is violated, identifying consumers for targeted rebalancing");
        Set<String> consumers = new HashSet<>();

        // Sort workers by load (highest first)
        List<WorkerLoad> sortedWorkers = workerLoads.stream()
                .sorted(Comparator.comparingInt((WorkerLoad w) -> w.tasks().size()).reversed())
                .collect(Collectors.toList());

        WorkerLoad highestLoadedWorker = sortedWorkers.get(0);
        WorkerLoad lowestLoadedWorker = sortedWorkers.get(sortedWorkers.size() - 1);
        int maxTasks = highestLoadedWorker.tasks().size();
        int minTasks = lowestLoadedWorker.tasks().size();
        int workerCount = workerLoads.size();

        log.debug("Highest loaded worker: {} with {} tasks, lowest loaded worker: {} with {} tasks",
                highestLoadedWorker.worker(), maxTasks, lowestLoadedWorker.worker(), minTasks);

        // Group connectors by consumer and count tasks per consumer
        Map<String, Integer> consumerTaskCounts = new HashMap<>();
        Map<String, Integer> consumerTasksMax = new HashMap<>();

        // Get tasks.max for each consumer
        for (String connector : configuredConnectors) {
            String consumer = extractConsumerFromConnector(connector);
            consumerTasksMax.computeIfAbsent(consumer, k -> getTasksMaxForConnector(connector));
        }

        // Count total tasks per consumer
        for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
            String consumer = entry.getKey();
            int totalTasks = entry.getValue().values().stream().mapToInt(Integer::intValue).sum();
            consumerTaskCounts.put(consumer, totalTasks);
        }

        // Sort consumers by ascending task count and tasks.max < workerCount
        List<String> sortedConsumers = consumerTaskCounts.entrySet().stream()
                .sorted((e1, e2) -> {
                    String c1 = e1.getKey();
                    String c2 = e2.getKey();

                    // Primary: prefer consumers with tasks.max < workerCount
                    boolean c1Small = consumerTasksMax.getOrDefault(c1, 1) < workerCount;
                    boolean c2Small = consumerTasksMax.getOrDefault(c2, 1) < workerCount;
                    if (c1Small != c2Small) return c1Small ? -1 : 1;

                    // Secondary: sort by task count ascending
                    return Integer.compare(e1.getValue(), e2.getValue());
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Find suitable consumers to rebalance
        for (String consumer : sortedConsumers) {
            int tasksMax = consumerTasksMax.getOrDefault(consumer, 1);

            // Skip if tasks.max >= workerCount (these are high-load consumers)
            if (tasksMax >= workerCount) continue;

            // Check if this consumer has more tasks on the highest loaded worker
            // than on the lowest loaded worker
            Map<String, Integer> workerCounts = taskCountByConsumerAndWorker.get(consumer);
            if (workerCounts != null) {
                int tasksOnHighWorker = workerCounts.getOrDefault(highestLoadedWorker.worker(), 0);
                int tasksOnLowWorker = workerCounts.getOrDefault(lowestLoadedWorker.worker(), 0);

                if (tasksOnHighWorker > tasksOnLowWorker) {
                    consumers.add(consumer);
                    log.debug("Adding consumer {} for rebalance (tasks.max={}, tasks on high={}, tasks on low={})",
                            consumer, tasksMax, tasksOnHighWorker, tasksOnLowWorker);

                    // Stop if we have enough consumers to fix the imbalance
                    if (maxTasks - minTasks <= consumers.size()) break;
                }
            }
        }

        // If we still need more consumers, try consumers with tasks.max < 2*workerCount
        if (consumers.isEmpty()) {
            for (String consumer : sortedConsumers) {
                int tasksMax = consumerTasksMax.getOrDefault(consumer, 1);
                if (tasksMax >= 2 * workerCount) {
                    continue;
                }
                Map<String, Integer> workerCounts = taskCountByConsumerAndWorker.get(consumer);
                if (workerCounts != null && workerCounts.getOrDefault(highestLoadedWorker.worker(), 0) > 0) {
                    consumers.add(consumer);
                    log.debug("Adding fallback consumer {} with tasks.max={}", consumer, tasksMax);
                    break;
                }
            }
        }

        return consumers;
    }

    /**
     * Helper method to perform rebalancing for a set of connectors and tasks
     */
    private ClusterAssignment performRebalanceForConnectors(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> currentMembers,
            Set<String> connectorsToRebalance,
            Set<ConnectorTaskId> tasksToRebalance,
            Set<ConnectorTaskId> allConfiguredTasks) {

        // 1. Create worker loads - excluding connectors & tasks to be rebalanced
        List<WorkerLoad> workerLoads = memberAssignments.entrySet().stream()
                .map(entry -> {
                    String worker = entry.getKey();
                    Collection<String> keptConnectors = entry.getValue().connectors().stream()
                            .filter(c -> !connectorsToRebalance.contains(c))
                            .collect(Collectors.toList());
                    Collection<ConnectorTaskId> keptTasks = entry.getValue().tasks().stream()
                            .filter(t -> !tasksToRebalance.contains(t))
                            .collect(Collectors.toList());
                    return new WorkerLoad.Builder(worker)
                            .with(keptConnectors, keptTasks)
                            .build();
                })
                .collect(Collectors.toList());

        // 2. Apply global balance assignment for connectors/tasks being rebalanced
        assignConnectors(workerLoads, connectorsToRebalance);
        assignTasks(workerLoads, tasksToRebalance);

        // 3. Calculate revoked resources (what each worker had but will lose)
        Map<String, Collection<String>> revokedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> revokedTasks = new HashMap<>();

        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();

            // Find connectors to revoke from this worker
            Set<String> connectors = entry.getValue().connectors().stream()
                    .filter(connectorsToRebalance::contains)
                    .collect(Collectors.toSet());
            if (!connectors.isEmpty()) {
                revokedConnectors.put(worker, connectors);
            }

            // Find tasks to revoke from this worker
            Set<ConnectorTaskId> tasks = entry.getValue().tasks().stream()
                    .filter(tasksToRebalance::contains)
                    .collect(Collectors.toSet());
            if (!tasks.isEmpty()) {
                revokedTasks.put(worker, tasks);
            }
        }

        // 4. Calculate newly assigned resources and final assignments
        Map<String, Collection<String>> finalConnectorAssignments = workerLoads.stream()
                .collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::connectors));

        Map<String, Collection<ConnectorTaskId>> finalTaskAssignments = workerLoads.stream()
                .collect(Collectors.toMap(WorkerLoad::worker, WorkerLoad::tasks));

        Map<String, Collection<String>> newlyAssignedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks = new HashMap<>();

        for (WorkerLoad worker : workerLoads) {
            String workerId = worker.worker();

            // Find newly assigned connectors
            Collection<String> newConnectors = worker.connectors().stream()
                    .filter(connectorsToRebalance::contains)
                    .collect(Collectors.toList());
            if (!newConnectors.isEmpty()) {
                newlyAssignedConnectors.put(workerId, newConnectors);
            }

            // Find newly assigned tasks
            Collection<ConnectorTaskId> newTasks = worker.tasks().stream()
                    .filter(tasksToRebalance::contains)
                    .collect(Collectors.toList());
            if (!newTasks.isEmpty()) {
                newlyAssignedTasks.put(workerId, newTasks);
            }
        }

        // Log summary of rebalance
        int revokedCount = revokedTasks.values().stream().mapToInt(Collection::size).sum();
        int assignedCount = newlyAssignedTasks.values().stream().mapToInt(Collection::size).sum();
        log.info("Rebalance complete: {} tasks revoked, {} tasks assigned",
                revokedCount, assignedCount);
        // Setting this to ERROR temporarily to ensure GlobalBalanceTaskAssignor is properly working
        // Setting it to info or warn produces too much data to track rebalancing activity
        log.error("INFO GlobalBalanceTaskAssignor: Rebalance complete: {} tasks revoked, {} tasks assigned",
                revokedCount, assignedCount);

        // Update previousMembers for next rebalance cycle
        previousMembers = currentMembers;

        // Return the rebalanced cluster assignment
        return new ClusterAssignment(
                newlyAssignedConnectors,
                newlyAssignedTasks,
                revokedConnectors,
                revokedTasks,
                finalConnectorAssignments,
                finalTaskAssignments
        );
    }

    /**
     * Gets the maximum tasks.max value for connectors in a consumer group.
     */
    private int getMaxTasksForConsumerGroup(String consumer, Map<String, List<String>> connectorsByConsumer) {
        List<String> connectors = connectorsByConsumer.getOrDefault(consumer, Collections.emptyList());
        return connectors.stream()
                .mapToInt(this::getTasksMaxForConnector)
                .max()
                .orElse(0);
    }

    private GlobalBalanceTaskAssignorScenarioType getTypeOfTaskBalanceScenario(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {

        // Initial Connector Allocation Check
        Set<String> currentlyAssignedConnectors = memberAssignments.values().stream()
                .flatMap(assignment -> assignment.connectors().stream())
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> currentlyAssignedTasks = memberAssignments.values().stream()
                .flatMap(assignment -> assignment.tasks().stream())
                .collect(Collectors.toSet());
        Set<String> newUnassignedConnectors = configuredConnectors.stream()
                .filter(connector -> !currentlyAssignedConnectors.contains(connector))
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> newUnassignedTasks = configuredTasks.stream()
                .filter(task -> !currentlyAssignedTasks.contains(task))
                .collect(Collectors.toSet());
        boolean hasNewWorkToAssign = !newUnassignedConnectors.isEmpty() || !newUnassignedTasks.isEmpty();

        // Check if this is the very first assignment (no previous members tracked)
        if (previousMembers == null || previousMembers.isEmpty()) {
            return GlobalBalanceTaskAssignorScenarioType.INITIAL_CONNECTOR_ALLOCATION;
        }

        if (hasNewWorkToAssign) {
            return GlobalBalanceTaskAssignorScenarioType.INITIAL_CONNECTOR_ALLOCATION;
        }

        Set<String> currentMembers = memberAssignments.keySet();

        // Scale Down Check
        boolean isfewerWorkers = currentMembers.size() < previousMembers.size();
        if (isfewerWorkers) {
            return GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_DOWN;
        }

        // Scale Up Check
        boolean moreWorkers = currentMembers.size() > previousMembers.size();
        if (moreWorkers) {
            return GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_UP;
        }

        // Worker Change Check
        boolean hasWorkersChanged = !currentMembers.containsAll(previousMembers) || !previousMembers.containsAll(currentMembers);
        if (hasWorkersChanged) {
            return GlobalBalanceTaskAssignorScenarioType.WORKER_CHANGED;
        }

        // Catch All
        return GlobalBalanceTaskAssignorScenarioType.OTHER;
    }

    private enum GlobalBalanceTaskAssignorScenarioType {
        /**
        SCENARIO: Initial Allocation new connector deployment
        A new consumer group deployment with no pre-existing task assignments.
        Tasks are allocated evenly among all available worker nodes.
            - Allocate equal number of tasks among all 5 available worker nodes
            - Per-consumer task count difference ≤ 1
            - Overall total tasks difference per worker ≤ 1
        **/
        INITIAL_CONNECTOR_ALLOCATION,

        /**  SCENARIO: Worker Scale Up Event
        Worker scale up event where 1 or more nodes are added to the cluster.
            - At all times, task difference for each consumer group ≤ 1 among all workers
            - Only trigger rebalance for consumer groups with task count > number of workers
            - Allocate equal amount of tasks on each node (difference ≤ 1)
            - Overall total tasks difference per worker ≤ 1
            - Sort consumers by task.max count and prioritize highest task count consumers
            - Revoke and move only the minimum required number of tasks to create balanced distribution
        **/
        WORKER_SCALE_UP,

        /**
        SCENARIO: Worker Scale Down Event
        Worker scale down event where 1 or more nodes are removed from the cluster.
            - Rebalance only the unassigned tasks from removed workers
            - Assume assigned tasks on remaining workers (W1, W2, W3) are already balanced
            - Allocate among remaining nodes in a balanced way
            - No task count difference > 1 for each consumer across all worker nodes
            - Overall total tasks difference per worker ≤ 1
            - Existing workload on W1, W2, W3 is preserved
        **/
        WORKER_SCALE_DOWN,

                /**
        SCENARIO: Worker Changed Event
        Worker changed event where 1 or more nodes are replaced during
        SCHEDULED_REBALANCE_MAX_DELAY_MS (scheduled.rebalance.max.delay.ms) window.
            - Behavior is identical to scale down event
            - Rebalance only the unassigned tasks from changed workers to the new ones
            - Remaining assigned tasks on existing workers are preserved
        **/
        WORKER_CHANGED,

        /**
        SCENARIO: Catch all logic as well as for configuration changes or redeployment event for existing consumer groups.
            - Check if tasks are balanced across all workers consumers and tasks or not.
            - Only trigger a rebalance if balance requirements are violated
                - Per-Consumer Balance: Task count difference across workers ≤ 1 for each consumer group
                - Global Balance: Total task count difference across workers ≤ 1
        **/
        OTHER
    }

}
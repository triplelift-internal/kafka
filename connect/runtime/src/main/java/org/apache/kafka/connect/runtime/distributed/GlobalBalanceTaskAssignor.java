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
        return 1; // Default value from ConnectorConfig.TASKS_MAX_DEFAULT
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
        log.error("GlobalBalanceTaskAssignor: Performing global balance task assignment for generation {}", currentGenerationId);


        this.configSnapshot = configSnapshot;
        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = configuredConnectors.stream()
                .flatMap(connector -> configSnapshot.tasks(connector).stream())
                .collect(Collectors.toSet());
        log.debug("Total configured connectors: {}, Total configured tasks: {}", 
                configuredConnectors.size(), configuredTasks.size());

        GlobalBalanceTaskAssignorScenarioType balanceScenarioType = getTypeOfTaskBalanceScenario(
                memberAssignments, configuredConnectors, configuredTasks);
        log.info("Detected balance scenario type: {}", balanceScenarioType);
        log.error("GlobalBalanceTaskAssignor: Detected balance scenario type: {}", balanceScenarioType);


        List<WorkerLoad> workerLoads = memberAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());
        log.debug("Created {} worker loads for assignment", workerLoads.size());

        if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.INITIAL_CONNECTOR_ALLOCATION) {
            return handleInitialConnectorAllocation(memberAssignments, workerLoads, configuredConnectors, configuredTasks);

        } else if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_DOWN || balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_CHANGED) {
            return handleWorkerScaleDownOrChanged(configSnapshot, memberAssignments, configuredConnectors, configuredTasks);

        } else if (balanceScenarioType == GlobalBalanceTaskAssignorScenarioType.WORKER_SCALE_UP) {
            return handleWorkerScaleUp(configSnapshot, lastCompletedGenerationId, currentGenerationId, memberAssignments, configuredConnectors, configuredTasks);

        } else {
            return handleOtherScenario(configSnapshot, lastCompletedGenerationId, currentGenerationId, memberAssignments, configuredConnectors, configuredTasks);
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
        log.error("GlobalBalanceTaskAssignor: Handling initial connector allocation scenario");

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
        
        // Verify and log the balance achieved
        //logBalanceVerification(workerLoads, newConnectors, newTasks);
        
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
        
        // Verify and log the balance achieved
        //logBalanceVerification(remainingWorkerLoads, unassignedConnectors, unassignedTasks);
        
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

        // Verify and log the balance achieved
        //logBalanceVerification(allWorkerLoads, connectorsToRebalance, tasksToRebalance);

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
        
        log.info("Handling other scenario - checking if current assignment is already balanced");

        Set<String> currentMembers = memberAssignments.keySet();

        // Step 1: Check if current assignment is balanced
        // Create WorkerLoad objects from current assignments to analyze balance
        List<WorkerLoad> currentWorkerLoads = memberAssignments.entrySet().stream()
                .map(entry -> new WorkerLoad.Builder(entry.getKey())
                        .with(entry.getValue().connectors(), entry.getValue().tasks())
                        .build())
                .collect(Collectors.toList());
        
        // Check global balance: total tasks difference ≤ 1 across all workers
        int minGlobalLoad = currentWorkerLoads.stream().mapToInt(w -> w.tasks().size()).min().orElse(0);
        int maxGlobalLoad = currentWorkerLoads.stream().mapToInt(w -> w.tasks().size()).max().orElse(0);
        boolean globalBalanceViolated = (maxGlobalLoad - minGlobalLoad) > 1;
        
        log.debug("Global balance check: min={}, max={}, difference={}, balanced={}", 
                minGlobalLoad, maxGlobalLoad, maxGlobalLoad - minGlobalLoad, !globalBalanceViolated);
        
        // Check per-consumer balance: task count difference ≤ 1 for each consumer group
        boolean perConsumerBalanceViolated = false;
        
        // Group tasks by consumer for balance checking
        Map<String, Map<String, Integer>> taskCountByConsumerAndWorker = new HashMap<>();
        
        // Initialize counts for each worker and consumer
        for (WorkerLoad worker : currentWorkerLoads) {
            String workerId = worker.worker();
            
            for (ConnectorTaskId task : worker.tasks()) {
                String consumer = extractConsumerFromConnector(task.connector());
                
                // Initialize consumer map if not present
                taskCountByConsumerAndWorker.computeIfAbsent(consumer, k -> new HashMap<>());
                
                // Initialize worker count for this consumer if not present
                Map<String, Integer> workerCounts = taskCountByConsumerAndWorker.get(consumer);
                workerCounts.put(workerId, workerCounts.getOrDefault(workerId, 0) + 1);
            }
        }
        
        // Ensure all workers have entries for all consumers (with count 0 if no tasks)
        for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
            Map<String, Integer> workerCounts = entry.getValue();
            for (WorkerLoad worker : currentWorkerLoads) {
                workerCounts.putIfAbsent(worker.worker(), 0);
            }
        }
        
        // Check balance for each consumer
        for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
            String consumer = entry.getKey();
            Map<String, Integer> workerCounts = entry.getValue();
            int minCount = Collections.min(workerCounts.values());
            int maxCount = Collections.max(workerCounts.values());
            
            if ((maxCount - minCount) > 1) {
                log.debug("Per-consumer balance violation for consumer {}: min={}, max={}, difference={}", 
                        consumer, minCount, maxCount, maxCount - minCount);
                perConsumerBalanceViolated = true;
                break;
            }
        }
        
        // If both global and per-consumer balance requirements are met, no rebalancing needed
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
            
            return new ClusterAssignment(
                    Collections.emptyMap(),       // no newly assigned connectors
                    Collections.emptyMap(),       // no newly assigned tasks
                    Collections.emptyMap(),       // no revocations needed
                    Collections.emptyMap(),       // no revocations needed
                    currentConnectorAssignments,  // current assignments preserved
                    currentTaskAssignments        // current assignments preserved
            );
        }
        
        // If we reach here, balance is violated and rebalancing is needed
        log.info("Balance requirements violated. Performing selective rebalance only for unbalanced consumer groups.");
        log.debug("Global balance violated: {}, Per-consumer balance violated: {}", 
                globalBalanceViolated, perConsumerBalanceViolated);
        
        // Identify which consumer groups have violated the balance constraint
        Set<String> unbalancedConsumers = new HashSet<>();
        
        // Check per-consumer balance violations
        for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
            String consumer = entry.getKey();
            Map<String, Integer> workerCounts = entry.getValue();
            int minCount = Collections.min(workerCounts.values());
            int maxCount = Collections.max(workerCounts.values());
            
            if ((maxCount - minCount) > 1) {
                unbalancedConsumers.add(consumer);
                log.debug("Consumer {} is unbalanced: min={}, max={}, difference={}", 
                        consumer, minCount, maxCount, maxCount - minCount);
            }
        }
        
        // If only global balance is violated but no specific consumer is unbalanced,
        // we need to do some targeted redistribution from low-load consumers
        if (globalBalanceViolated && unbalancedConsumers.isEmpty()) {
            log.debug("Only global balance is violated, identifying low-load consumers for targeted rebalancing");
            
            // Get workers sorted by load (highest first and lowest last)
            List<WorkerLoad> workersSortedByLoad = currentWorkerLoads.stream()
                    .sorted(Comparator.comparingInt((WorkerLoad w) -> w.tasks().size()).reversed())
                    .collect(Collectors.toList());
            
            // Get the highest and lowest loaded workers
            WorkerLoad highestLoadedWorker = workersSortedByLoad.get(0);
            WorkerLoad lowestLoadedWorker = workersSortedByLoad.get(workersSortedByLoad.size() - 1);
            
            int maxTasks = highestLoadedWorker.tasks().size();
            int minTasks = lowestLoadedWorker.tasks().size();
            int currentWorkerCount = currentWorkerLoads.size();
            
            log.debug("Highest loaded worker: {} with {} tasks, lowest loaded worker: {} with {} tasks", 
                    highestLoadedWorker.worker(), maxTasks, lowestLoadedWorker.worker(), minTasks);
            
            // Calculate task count for each consumer group
            Map<String, Integer> consumerTaskCounts = new HashMap<>();
            Map<String, Integer> consumerTasksMax = new HashMap<>();
            
            for (String connector : configuredConnectors) {
                String consumer = extractConsumerFromConnector(connector);
                consumerTasksMax.computeIfAbsent(consumer, k -> getTasksMaxForConnector(connector));
            }
            
            for (Map.Entry<String, Map<String, Integer>> entry : taskCountByConsumerAndWorker.entrySet()) {
                String consumer = entry.getKey();
                Map<String, Integer> workerCounts = entry.getValue();
                int totalTasks = workerCounts.values().stream()
                        .mapToInt(Integer::intValue).sum();
                consumerTaskCounts.put(consumer, totalTasks);
            }
            
            // Sort consumers by task count (ascending - lowest task count first)
            List<String> sortedConsumersByTaskCount = consumerTaskCounts.entrySet().stream()
                    .sorted((e1, e2) -> {
                        String c1 = e1.getKey();
                        String c2 = e2.getKey();
                        int count1 = e1.getValue();
                        int count2 = e2.getValue();
                        
                        // Primary sort: consumers with task.max < workerCount come first
                        boolean c1SmallerThanWorkers = consumerTasksMax.getOrDefault(c1, 1) < currentWorkerCount;
                        boolean c2SmallerThanWorkers = consumerTasksMax.getOrDefault(c2, 1) < currentWorkerCount;
                        
                        if (c1SmallerThanWorkers != c2SmallerThanWorkers) {
                            return c1SmallerThanWorkers ? -1 : 1;
                        }
                        
                        // Secondary sort: by task count (ascending)
                        return Integer.compare(count1, count2);
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            
            // Identify which low-load consumers to move
            for (String consumer : sortedConsumersByTaskCount) {
                int tasksMax = consumerTasksMax.getOrDefault(consumer, 1);
                
                // Skip high-load consumers (task.max > worker count)
                if (tasksMax >= currentWorkerCount) {
                    log.debug("Skipping consumer {} with tasks.max={} >= worker count {}", 
                            consumer, tasksMax, currentWorkerCount);
                    continue;
                }
                
                // Check if this consumer has more tasks on the highest loaded worker
                // than on the lowest loaded worker
                int tasksOnHighWorker = 0;
                int tasksOnLowWorker = 0;
                
                if (taskCountByConsumerAndWorker.containsKey(consumer)) {
                    Map<String, Integer> workerCounts = taskCountByConsumerAndWorker.get(consumer);
                    tasksOnHighWorker = workerCounts.getOrDefault(highestLoadedWorker.worker(), 0);
                    tasksOnLowWorker = workerCounts.getOrDefault(lowestLoadedWorker.worker(), 0);
                }
                
                if (tasksOnHighWorker > tasksOnLowWorker) {
                    unbalancedConsumers.add(consumer);
                    log.debug("Adding low-load consumer {} with tasks.max={} to rebalance list (tasks on high={}, tasks on low={})", 
                            consumer, tasksMax, tasksOnHighWorker, tasksOnLowWorker);
                    
                    // If we have enough consumers to rebalance, stop adding more
                    if (maxTasks - minTasks <= unbalancedConsumers.size()) {
                        break;
                    }
                }
            }
            
            // If we still haven't found enough consumers to rebalance, try with consumers
            // that have tasks.max < 2*workerCount as a fallback
            if (unbalancedConsumers.isEmpty()) {
                log.debug("No suitable low-load consumers found, trying consumers with tasks.max < 2*workerCount");
                
                for (String consumer : sortedConsumersByTaskCount) {
                    int tasksMax = consumerTasksMax.getOrDefault(consumer, 1);
                    
                    // Only consider consumers with tasks.max < 2*workerCount
                    if (tasksMax >= 2 * currentWorkerCount) {
                        continue;
                    }
                    
                    // Check if this consumer has tasks on the highest loaded worker
                    if (taskCountByConsumerAndWorker.containsKey(consumer)) {
                        Map<String, Integer> workerCounts = taskCountByConsumerAndWorker.get(consumer);
                        int tasksOnHighWorker = workerCounts.getOrDefault(highestLoadedWorker.worker(), 0);
                        
                        if (tasksOnHighWorker > 0) {
                            unbalancedConsumers.add(consumer);
                            log.debug("Adding fallback consumer {} with tasks.max={} to rebalance list", 
                                    consumer, tasksMax);
                            break;
                        }
                    }
                }
            }
        }
        
        // Create new WorkerLoad objects that preserve current assignments
        List<WorkerLoad> newWorkerLoads = currentMembers.stream()
                .map(worker -> new WorkerLoad.Builder(worker).build())
                .collect(Collectors.toList());
                
        // Filter connectors and tasks that belong to unbalanced consumers
        Set<String> connectorsToRebalance = new HashSet<>();
        Set<ConnectorTaskId> tasksToRebalance = new HashSet<>();
        
        // Identify connectors and tasks from unbalanced consumer groups
        for (String connector : configuredConnectors) {
            String consumer = extractConsumerFromConnector(connector);
            if (unbalancedConsumers.contains(consumer)) {
                connectorsToRebalance.add(connector);
            }
        }
        
        for (ConnectorTaskId task : configuredTasks) {
            String consumer = extractConsumerFromConnector(task.connector());
            if (unbalancedConsumers.contains(consumer)) {
                tasksToRebalance.add(task);
            }
        }
        
        log.debug("Selective rebalance will reassign {} connectors and {} tasks from {} unbalanced consumers", 
                connectorsToRebalance.size(), tasksToRebalance.size(), unbalancedConsumers.size());
        
        // Assign only the connectors and tasks from unbalanced consumers
        assignConnectors(newWorkerLoads, connectorsToRebalance);
        assignTasks(newWorkerLoads, tasksToRebalance);

        // Build final assignments
        Map<String, Collection<String>> finalConnectorAssignments = newWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));

        Map<String, Collection<ConnectorTaskId>> finalTaskAssignments = newWorkerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::tasks
                ));

        // Find connectors and tasks that changed assignment
        Map<String, Collection<String>> newlyAssignedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> newlyAssignedTasks = new HashMap<>();
        Map<String, Collection<String>> revokedConnectors = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> revokedTasks = new HashMap<>();
        
        // Track current and new assignments for comparison
        Map<String, Set<String>> currentConnectorsByWorker = new HashMap<>();
        Map<String, Set<ConnectorTaskId>> currentTasksByWorker = new HashMap<>();
        Map<String, Set<String>> newConnectorsByWorker = new HashMap<>();
        Map<String, Set<ConnectorTaskId>> newTasksByWorker = new HashMap<>();
        
        // Populate current assignments
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String workerId = entry.getKey();
            currentConnectorsByWorker.put(workerId, new HashSet<>(entry.getValue().connectors()));
            currentTasksByWorker.put(workerId, new HashSet<>(entry.getValue().tasks()));
        }
        
        // Populate new assignments
        for (WorkerLoad worker : newWorkerLoads) {
            String workerId = worker.worker();
            newConnectorsByWorker.put(workerId, new HashSet<>(worker.connectors()));
            newTasksByWorker.put(workerId, new HashSet<>(worker.tasks()));
        }
        
        // Calculate revoked and newly assigned connectors and tasks
        for (String workerId : currentMembers) {
            Set<String> currentConnectors = currentConnectorsByWorker.getOrDefault(workerId, Collections.emptySet());
            Set<String> newConnectors = newConnectorsByWorker.getOrDefault(workerId, Collections.emptySet());
            Set<ConnectorTaskId> currentTasks = currentTasksByWorker.getOrDefault(workerId, Collections.emptySet());
            Set<ConnectorTaskId> newTasks = newTasksByWorker.getOrDefault(workerId, Collections.emptySet());
            
            // Find revoked (current - new)
            Set<String> revoked = new HashSet<>(currentConnectors);
            revoked.removeAll(newConnectors);
            if (!revoked.isEmpty()) {
                revokedConnectors.put(workerId, revoked);
            }
            
            Set<ConnectorTaskId> revokedT = new HashSet<>(currentTasks);
            revokedT.removeAll(newTasks);
            if (!revokedT.isEmpty()) {
                revokedTasks.put(workerId, revokedT);
            }
            
            // Find newly assigned (new - current)
            Set<String> newlyAssigned = new HashSet<>(newConnectors);
            newlyAssigned.removeAll(currentConnectors);
            if (!newlyAssigned.isEmpty()) {
                newlyAssignedConnectors.put(workerId, newlyAssigned);
            }
            
            Set<ConnectorTaskId> newlyAssignedT = new HashSet<>(newTasks);
            newlyAssignedT.removeAll(currentTasks);
            if (!newlyAssignedT.isEmpty()) {
                newlyAssignedTasks.put(workerId, newlyAssignedT);
            }
        }

        log.debug("Selective rebalance complete - {} connectors and {} tasks reassigned across {} workers", 
                connectorsToRebalance.size(), tasksToRebalance.size(), newWorkerLoads.size());

        // Verify and log the balance achieved
        //logBalanceVerification(newWorkerLoads, connectorsToRebalance, tasksToRebalance);
        
        // Log rebalance results
        int revoked = revokedTasks.values().stream().mapToInt(Collection::size).sum();
        int assigned = newlyAssignedTasks.values().stream().mapToInt(Collection::size).sum();
        log.info("Selective rebalance results: {} tasks revoked, {} tasks assigned", revoked, assigned);

        // Update previousMembers for next rebalance cycle
        previousMembers = currentMembers;

        // Return the cluster assignment for selective rebalance
        return new ClusterAssignment(
                newlyAssignedConnectors,    // only connectors that changed assignment
                newlyAssignedTasks,         // only tasks that changed assignment
                revokedConnectors,          // only revoked assignments 
                revokedTasks,               // only revoked assignments
                finalConnectorAssignments,  // final state after selective rebalance
                finalTaskAssignments        // final state after selective rebalance
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
        SCENARIO: Catch All Scenario 
        Consumer group changes, configuration changes and other unhandled scenarios.
            - Trigger Incremental Cooperative Rebalance
        **/
        OTHER
    }

}
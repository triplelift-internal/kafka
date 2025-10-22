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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Balanced Cooperative Task Assignor for Kafka Connect distributed mode.
 * 
 * <p>Provides fast-converging, balanced task distribution using the cooperative rebalancing protocol.
 * Designed for large-scale, multi-tenant environments with varying connector task loads and 
 * frequent worker churn in cloud-native deployments (e.g. AWS Spot, Azure Spot, GCP Preemptible instances).
 * Optimized for autoscaling platforms including Kubernetes HPA, AWS Auto Scaling, Azure VMSS, and GCP MIGs.
 * 
 * <h2>Key Features</h2>
 * <ul>
 *   <li><b>Fast Convergence:</b> Maximum 2 rebalance generations for scale-up, 1 generation for scale-down or task changes</li>
 *   <li><b>Per-Connector Fairness:</b> Each connector's tasks balanced across workers using consumerNTaskCountMin/consumerNTaskCountMax targets</li>
 *   <li><b>Global Load Balance:</b> Total task distribution balanced using globalMin/globalMax/globalMaxLimit thresholds</li>
 *   <li><b>Minimal Disruption:</b> Preserves existing task assignments whenever possible</li>
 *   <li><b>Cloud-Native Resilience:</b> Handles spot/preemptible instance volatility and rapid autoscaling events</li>
 * </ul>
 * 
 * <h2>Two-Round Rebalancing Protocol</h2>
 * 
 * <h3>Round 1: Full Revocation (Conditional)</h3>
 * <p>Triggered only when necessary to ensure optimal redistribution:
 * <ul>
 *   <li><b>Scale-Up Detection:</b> Any worker has zero assigned tasks (new worker joined)</li>
 *   <li><b>Severe Imbalance:</b> Any worker exceeds globalMaxLimit threshold</li>
 * </ul>
 * <p>When triggered, all tasks are revoked from all workers to enable optimal redistribution in Round 2.
 * 
 * <h3>Round 2: Complete Assignment</h3>
 * <p>Assigns all unassigned tasks while maintaining balance constraints. Consists of two phases:
 * 
 * <h4>Phase A: Fill to consumerNTaskCountMin</h4>
 * <p>Ensures minimum task guarantee per connector. Uses 1-task-at-a-time assignment with worker re-sorting
 * after each assignment to maintain perfect balance throughout the process.
 * 
 * <h4>Phase B: Distribute Remaining</h4>
 * <p>Distributes remaining tasks while respecting consumerNTaskCountMax and globalMaxLimit constraints.
 * Special handling for small connectors (task count ≤ worker count) to prevent over-concentration.
 * 
 * <h2>Optimization Behaviors</h2>
 * <ul>
 *   <li>Round 1 is <b>not</b> triggered for scale-down, task count changes, or connector deletions</li>
 *   <li>Tasks from deleted connectors are automatically filtered during worker state construction</li>
 *   <li>Existing assignments are preserved when cluster is already balanced</li>
 * </ul>
 * 
 * <h2>Supporting Components</h2>
 * <ul>
 *   <li>{@link WorkerState} - Tracks assigned tasks for each worker</li>
 *   <li>{@link BalanceTargets} - Global and per-connector balance targets</li>
 *   <li>{@link ConsumerTarget} - consumerNTaskCountMin/consumerNTaskCountMax balance targets for individual connectors</li>
 *   <li>{@link ConnectorAssignments} - Connector assignment computation results</li>
 *   <li>{@link DeletionInfo} - Revocation tracking for deleted connectors and tasks</li>
 * </ul>
 * 
 * @see IncrementalCooperativeAssignor
 */
public class BalancedCooperativeAssignor extends IncrementalCooperativeAssignor {
    private final Logger log;

    // For testing
    ClusterConfigState configSnapshot;

    public BalancedCooperativeAssignor(LogContext logContext, Time time, int maxDelay) {
        super(logContext, time, maxDelay);
        this.log = logContext.logger(BalancedCooperativeAssignor.class);
    }

    @Override
    protected ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        this.configSnapshot = configSnapshot;
        
        log.info("=== Starting BalancedCooperativeAssignor for generation {} (previous: {}) ===", 
                currentGenerationId, lastCompletedGenerationId);
        
        // Step 1: Build current state from ConfigSnapshot
        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = new TreeSet<>();
        for (String connector : configuredConnectors) {
            configuredTasks.addAll(configSnapshot.tasks(connector));
        }
        
        log.info("ConfigSnapshot - Connectors: {}, Tasks: {}, Workers: {}", 
                configuredConnectors.size(), configuredTasks.size(), memberAssignments.size());
        
        // Build worker state with automatic filtering of deleted tasks
        List<WorkerState> workers = buildWorkerState(memberAssignments, configuredTasks, configuredConnectors);
        
        if (workers.isEmpty()) {
            log.warn("No workers available for assignment");
            return ClusterAssignment.EMPTY;
        }
        
        // Calculate balance targets
        BalanceTargets targets = calculateBalanceTargets(workers, configuredConnectors, configuredTasks);
        
        // Identify unassigned tasks
        Set<ConnectorTaskId> allAssignedTasks = workers.stream()
                .flatMap(w -> w.assignedTasks.stream())
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> unassignedTasks = new TreeSet<>(configuredTasks);
        unassignedTasks.removeAll(allAssignedTasks);
        
        log.info("Current state - Assigned: {}, Unassigned: {}", allAssignedTasks.size(), unassignedTasks.size());
        
        // Step 2: Check Round 1 Triggers and decide action
        boolean triggerRound1 = shouldTriggerRound1(workers, targets);
        boolean hasTasksToRevoke = allAssignedTasks.size() > 0;
        
        Map<String, Collection<ConnectorTaskId>> tasksToRevoke = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> tasksToAssign = new HashMap<>();
        
        // Cooperative Protocol Rule: A single rebalance can EITHER revoke OR assign, not both
        
        if (triggerRound1 && hasTasksToRevoke) {
            // Round 1: Full Revocation (only if there are tasks to revoke)
            log.info("=== ROUND 1: Full Revocation Triggered ===");
            tasksToRevoke = performRound1FullRevocation(workers);
            
            // Update worker state after revocation
            for (WorkerState worker : workers) {
                Collection<ConnectorTaskId> revoked = tasksToRevoke.getOrDefault(worker.worker, Collections.emptyList());
                worker.assignedTasks.removeAll(revoked);
                unassignedTasks.addAll(revoked);
            }
            
            log.info("Round 1 complete - Revoked: {} tasks from {} workers", 
                    tasksToRevoke.values().stream().mapToInt(Collection::size).sum(),
                    tasksToRevoke.size());
            
            // Cooperative protocol: After revocations, we cannot assign in the same generation
            // Workers will rejoin and next rebalance will perform Round 2 assignments
            
        } else if (!unassignedTasks.isEmpty()) {
            // Round 2: Complete Assignment
            // This happens when:
            // 1. Round 1 would trigger but there's nothing to revoke (empty workers scenario)
            // 2. Round 1 not triggered and there are unassigned tasks
            log.info("=== ROUND 2: Complete Assignment (skipping Round 1) ===");
            tasksToAssign = performRound2CompleteAssignment(workers, unassignedTasks, targets);
            
            log.info("Round 2 complete - Assigned: {} tasks to {} workers",
                    tasksToAssign.values().stream().mapToInt(Collection::size).sum(),
                    tasksToAssign.size());
        } else {
            log.info("Cluster is balanced, no actions needed");
        }
        
        // Step 3: Build final task assignments
        Map<String, Collection<ConnectorTaskId>> allTaskAssignments = buildFinalTaskAssignments(
                workers, memberAssignments, tasksToRevoke, tasksToAssign, configuredConnectors);
        
        // Step 4: Handle connector assignments
        ConnectorAssignments connectorAssignments = buildConnectorAssignments(
                workers, memberAssignments, allTaskAssignments, configuredConnectors);
        
        // Step 5: Handle deleted connectors and tasks
        DeletionInfo deletionInfo = identifyDeletions(memberAssignments, configuredConnectors, configuredTasks);
        
        // Merge deleted connector tasks with other task revocations
        Map<String, Collection<ConnectorTaskId>> allTaskRevocations = mergeMaps(tasksToRevoke, deletionInfo.taskRevocations);
        
        // Update state for next round
        previousMembers = new TreeSet<>(memberAssignments.keySet());
        previousGenerationId = currentGenerationId;
        
        log.info("=== Assignment Complete - Connector Revocations: {}, Task Revocations: {}, Connector Assignments: {}, Task Assignments: {} ===",
                deletionInfo.connectorRevocations.values().stream().mapToInt(Collection::size).sum(),
                allTaskRevocations.values().stream().mapToInt(Collection::size).sum(),
                connectorAssignments.newlyAssigned.values().stream().mapToInt(Collection::size).sum(),
                tasksToAssign.values().stream().mapToInt(Collection::size).sum());
        
        return new ClusterAssignment(
                connectorAssignments.newlyAssigned,
                tasksToAssign,
                deletionInfo.connectorRevocations,
                allTaskRevocations,
                connectorAssignments.allAssigned,
                allTaskAssignments
        );
    }
    
    // ==================== Round 1: Full Revocation ====================
    /**
     * Check if Round 1 (full revocation) should be triggered.
     * 
     * Round 1 is triggered if ANY of these conditions are met:
     * 1. Scale-Up Detection: Any worker has 0 assigned tasks
     * 2. Severe Imbalance: Any worker has > globalMaxLimit tasks
     */
    private boolean shouldTriggerRound1(List<WorkerState> workers, BalanceTargets targets) {
        for (WorkerState worker : workers) {
            int taskCount = worker.assignedTasks.size();
            
            // Trigger 1: Empty worker detected (scale-up scenario)
            if (taskCount == 0) {
                log.info("Round 1 Trigger: Empty worker detected ({})", worker.worker);
                return true;
            }
            
            // Trigger 2: Worker exceeds globalMaxLimit (severe imbalance)
            if (taskCount > targets.globalMaxLimit) {
                log.info("Round 1 Trigger: Severe imbalance detected - worker {} has {} tasks (limit: {})",
                        worker.worker, taskCount, targets.globalMaxLimit);
                return true;
            }
        }
        
        log.info("Round 1 NOT triggered - proceeding to Round 2 for incremental assignment");
        return false;
    }
    
    /**
     * Perform Round 1: Full revocation of all tasks from all workers.
     * 
     * This prepares for optimal redistribution in Round 2.
     */
    private Map<String, Collection<ConnectorTaskId>> performRound1FullRevocation(List<WorkerState> workers) {
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        for (WorkerState worker : workers) {
            if (!worker.assignedTasks.isEmpty()) {
                List<ConnectorTaskId> allTasks = new ArrayList<>(worker.assignedTasks);
                Collections.sort(allTasks);
                revocations.put(worker.worker, allTasks);
                
                log.debug("Round 1 - Revoking {} tasks from worker {}", allTasks.size(), worker.worker);
            }
        }
        
        return revocations;
    }
    
    // ==================== Round 2: Complete Assignment ====================
    /**
     * Perform Round 2: Complete assignment of all unassigned tasks.
     * 
     * Consists of two phases:
     * - Phase A: Fill to consumerNTaskCountMin (minimum guarantee)
     * - Phase B: Distribute remaining tasks
     */
    private Map<String, Collection<ConnectorTaskId>> performRound2CompleteAssignment(
            List<WorkerState> workers,
            Set<ConnectorTaskId> unassignedTasks,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> assignments = new HashMap<>();
        
        // Group unassigned tasks by connector
        Map<String, List<ConnectorTaskId>> unassignedByConsumer = new TreeMap<>();
        for (ConnectorTaskId task : unassignedTasks) {
            unassignedByConsumer.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }
        
        // Sort tasks for determinism
        unassignedByConsumer.values().forEach(Collections::sort);
        
        log.info("Round 2 - Starting with {} unassigned tasks across {} consumers",
                unassignedTasks.size(), unassignedByConsumer.size());
        
        // Phase A: Fill to consumerNTaskCountMin
        performPhaseA_FillToConsumerNTaskCountMin(workers, unassignedByConsumer, targets, assignments);
        
        // Phase B: Distribute remaining tasks
        performPhaseB_DistributeRemaining(workers, unassignedByConsumer, targets, assignments);
        
        return assignments;
    }
    
    /**
     * Phase A: Fill to consumerNTaskCountMin (Minimum Guarantee)
     * 
     * Ensure every worker gets at least consumerNTaskCountMin tasks from each consumer before distributing extras.
     * Strategy: 1-task-at-a-time assignment with re-sorting to maintain perfect balance throughout.
     */
    private void performPhaseA_FillToConsumerNTaskCountMin(
            List<WorkerState> workers,
            Map<String, List<ConnectorTaskId>> unassignedByConsumer,
            BalanceTargets targets,
            Map<String, Collection<ConnectorTaskId>> assignments) {
        
        log.info("Phase A: Fill to consumerNTaskCountMin");
        
        // Sort consumers by consumerNTaskCountMax (descending) - prioritize high consumerNTaskCountMax first
        List<String> sortedConsumers = unassignedByConsumer.keySet().stream()
                .sorted((c1, c2) -> Integer.compare(
                        targets.perConsumerTargets.get(c2).consumerNTaskCountMax,
                        targets.perConsumerTargets.get(c1).consumerNTaskCountMax))
                .collect(Collectors.toList());
        
        for (String consumer : sortedConsumers) {
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            List<ConnectorTaskId> unassigned = unassignedByConsumer.get(consumer);
            
            if (unassigned.isEmpty() || target.consumerNTaskCountMin == 0) {
                continue;
            }
            
            log.debug("Phase A - Consumer: {}, consumerNTaskCountMin: {}, unassigned: {}", 
                    consumer, target.consumerNTaskCountMin, unassigned.size());
            
            // Fill each worker to consumerNTaskCountMin, re-sorting after each task assignment
            boolean progress = true;
            while (progress && !unassigned.isEmpty()) {
                progress = false;
                
                // Sort workers by total load (ascending - least loaded first)
                workers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
                
                for (WorkerState worker : workers) {
                    int currentCount = worker.getTaskCountForConnector(consumer);
                    
                    if (currentCount < target.consumerNTaskCountMin && !unassigned.isEmpty()) {
                        // Assign EXACTLY 1 task
                        ConnectorTaskId task = unassigned.remove(0);
                        
                        assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(task);
                        worker.assignedTasks.add(task);
                        
                        progress = true;
                        
                        log.trace("Phase A - Assigned task {} to worker {} (now has {} from {})",
                                task, worker.worker, currentCount + 1, consumer);
                        
                        // Break and re-sort after each task assignment (critical for balance)
                        break;
                    }
                }
            }
        }
        
        int remaining = unassignedByConsumer.values().stream().mapToInt(List::size).sum();
        log.info("Phase A complete - Remaining unassigned: {}", remaining);
    }
    
    /**
     * Phase B: Distribute Remaining Tasks
     * 
     * Distribute all remaining unassigned tasks while respecting consumerNTaskCountMax and globalMaxLimit.
     * Strategy: 1-task-at-a-time assignment to least-loaded eligible workers.
     */
    private void performPhaseB_DistributeRemaining(
            List<WorkerState> workers,
            Map<String, List<ConnectorTaskId>> unassignedByConsumer,
            BalanceTargets targets,
            Map<String, Collection<ConnectorTaskId>> assignments) {
        
        log.info("Phase B: Distribute remaining tasks");
        
        // Sort consumers by remaining task count (descending) - prioritize consumers with most remaining tasks
        while (unassignedByConsumer.values().stream().anyMatch(l -> !l.isEmpty())) {
            List<String> sortedConsumers = unassignedByConsumer.entrySet().stream()
                    .filter(e -> !e.getValue().isEmpty())
                    .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            
            if (sortedConsumers.isEmpty()) {
                break;
            }
            
            boolean progress = false;
            
            for (String consumer : sortedConsumers) {
                List<ConnectorTaskId> unassigned = unassignedByConsumer.get(consumer);
                ConsumerTarget target = targets.perConsumerTargets.get(consumer);
                boolean isSmallConsumer = target.totalTasks <= workers.size();
                
                if (unassigned.isEmpty()) {
                    continue;
                }
                
                // Sort workers by total load (ascending - least loaded first)
                workers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
                
                // Find first eligible worker
                WorkerState selectedWorker = null;
                for (WorkerState worker : workers) {
                    int currentCount = worker.getTaskCountForConnector(consumer);
                    
                    // Check eligibility criteria
                    boolean withinGlobalLimit = worker.assignedTasks.size() < targets.globalMaxLimit;
                    boolean withinConsumerMax = currentCount < target.consumerNTaskCountMax;
                    boolean withinSmallConsumerLimit = !isSmallConsumer || currentCount < 1;
                    
                    if (withinGlobalLimit && withinConsumerMax && withinSmallConsumerLimit) {
                        selectedWorker = worker;
                        break;
                    }
                }
                
                if (selectedWorker == null) {
                    if (isSmallConsumer) {
                        log.debug("Phase B - Cannot assign remaining {} tasks for small consumer {} - all workers either at globalMaxLimit or already have 1 task",
                                unassigned.size(), consumer);
                    } else {
                        log.warn("Phase B - Cannot assign remaining {} tasks for consumer {} - all workers at capacity",
                                unassigned.size(), consumer);
                    }
                    // Remove from unassigned to avoid infinite loop
                    unassigned.clear();
                    continue;
                }
                
                // Assign EXACTLY 1 task
                ConnectorTaskId task = unassigned.remove(0);
                assignments.computeIfAbsent(selectedWorker.worker, k -> new ArrayList<>()).add(task);
                selectedWorker.assignedTasks.add(task);
                
                progress = true;
                
                log.trace("Phase B - Assigned task {} to worker {} (total: {}, from {}: {})",
                        task, selectedWorker.worker, selectedWorker.assignedTasks.size(),
                        consumer, selectedWorker.getTaskCountForConnector(consumer));
            }
            
            if (!progress) {
                // No progress made, exit to avoid infinite loop
                int remaining = unassignedByConsumer.values().stream().mapToInt(List::size).sum();
                if (remaining > 0) {
                    log.warn("Phase B - Stopping with {} unassigned tasks (no eligible workers found)", remaining);
                }
                break;
            }
        }
        
        int remaining = unassignedByConsumer.values().stream().mapToInt(List::size).sum();
        log.info("Phase B complete - Remaining unassigned: {}", remaining);
    }
    
    // ==================== Helper Methods ====================
    /**
     * Build worker state from member assignments, filtering out tasks from deleted connectors.
     */
    private List<WorkerState> buildWorkerState(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<ConnectorTaskId> configuredTasks,
            Set<String> configuredConnectors) {
        
        List<WorkerState> workers = new ArrayList<>();
        
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            WorkerState worker = new WorkerState(entry.getKey());
            
            // Filter out tasks belonging to deleted connectors
            List<ConnectorTaskId> validTasks = entry.getValue().tasks().stream()
                    .filter(task -> configuredConnectors.contains(task.connector()))
                    .collect(Collectors.toList());
            
            worker.assignedTasks.addAll(validTasks);
            workers.add(worker);
        }
        
        return workers;
    }
    
    /**
     * Calculate balance targets for global and per-consumer distribution.
     */
    private BalanceTargets calculateBalanceTargets(
            List<WorkerState> workers,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {
        
        BalanceTargets targets = new BalanceTargets();
        int numWorkers = workers.size();
        
        if (numWorkers == 0) {
            return targets;
        }
        
        // Global targets
        targets.totalTasks = configuredTasks.size();
        targets.globalMin = targets.totalTasks / numWorkers;
        targets.globalMax = (targets.totalTasks + numWorkers - 1) / numWorkers;  // ceiling
        targets.globalMaxLimit = targets.globalMax + 1;
        
        log.debug("Global targets - totalTasks: {}, globalMin: {}, globalMax: {}, globalMaxLimit: {}",
                targets.totalTasks, targets.globalMin, targets.globalMax, targets.globalMaxLimit);
        
        // Per-consumer targets
        Map<String, Integer> taskCountsByConnector = new TreeMap<>();
        for (ConnectorTaskId task : configuredTasks) {
            taskCountsByConnector.merge(task.connector(), 1, Integer::sum);
        }
        
        for (String connector : configuredConnectors) {
            int taskCount = taskCountsByConnector.getOrDefault(connector, 0);
            ConsumerTarget target = new ConsumerTarget();
            target.totalTasks = taskCount;
            target.consumerNTaskCountMin = taskCount / numWorkers;
            target.consumerNTaskCountMax = (taskCount + numWorkers - 1) / numWorkers;  // ceiling
            targets.perConsumerTargets.put(connector, target);
            
            log.debug("Consumer targets - {}: totalTasks={}, consumerNTaskCountMin={}, consumerNTaskCountMax={}",
                    connector, taskCount, target.consumerNTaskCountMin, target.consumerNTaskCountMax);
        }
        
        return targets;
    }
    
    /**
     * Build final task assignments for all workers.
     */
    private Map<String, Collection<ConnectorTaskId>> buildFinalTaskAssignments(
            List<WorkerState> workers,
            Map<String, ConnectorsAndTasks> memberAssignments,
            Map<String, Collection<ConnectorTaskId>> tasksToRevoke,
            Map<String, Collection<ConnectorTaskId>> tasksToAssign,
            Set<String> configuredConnectors) {
        
        Map<String, Collection<ConnectorTaskId>> allTaskAssignments = new HashMap<>();
        
        for (WorkerState worker : workers) {
            Set<ConnectorTaskId> finalTasks = new TreeSet<>(
                    memberAssignments.getOrDefault(worker.worker, ConnectorsAndTasks.EMPTY).tasks());
            
            // Remove revoked tasks
            finalTasks.removeAll(tasksToRevoke.getOrDefault(worker.worker, Collections.emptyList()));
            
            // Add assigned tasks
            finalTasks.addAll(tasksToAssign.getOrDefault(worker.worker, Collections.emptyList()));
            
            // Filter out tasks belonging to deleted connectors
            finalTasks.removeIf(task -> !configuredConnectors.contains(task.connector()));
            
            allTaskAssignments.put(worker.worker, new ArrayList<>(finalTasks));
        }
        
        return allTaskAssignments;
    }
    
    /**
     * Build connector assignments using round-robin for unassigned connectors.
     */
    private ConnectorAssignments buildConnectorAssignments(
            List<WorkerState> workers,
            Map<String, ConnectorsAndTasks> memberAssignments,
            Map<String, Collection<ConnectorTaskId>> allTaskAssignments,
            Set<String> configuredConnectors) {
        
        ConnectorAssignments result = new ConnectorAssignments();
        
        // Build WorkerLoad with existing connector assignments
        List<WorkerLoad> workerLoads = new ArrayList<>();
        Set<String> alreadyAssignedConnectors = new HashSet<>();
        
        for (WorkerState worker : workers) {
            Collection<String> existingConnectors = memberAssignments
                    .getOrDefault(worker.worker, ConnectorsAndTasks.EMPTY)
                    .connectors();
            
            // Filter out connectors that no longer exist
            List<String> validConnectors = existingConnectors.stream()
                    .filter(configuredConnectors::contains)
                    .collect(Collectors.toList());
            
            alreadyAssignedConnectors.addAll(validConnectors);
            
            workerLoads.add(new WorkerLoad.Builder(worker.worker)
                    .with(validConnectors, allTaskAssignments.getOrDefault(worker.worker, Collections.emptyList()))
                    .build());
        }
        
        // Determine which connectors need assignment
        Set<String> connectorsToAssign = new TreeSet<>(configuredConnectors);
        connectorsToAssign.removeAll(alreadyAssignedConnectors);
        
        log.debug("Connectors - Already assigned: {}, To assign: {}",
                alreadyAssignedConnectors.size(), connectorsToAssign.size());
        
        // Assign unassigned connectors using round-robin
        if (!connectorsToAssign.isEmpty()) {
            assignConnectors(workerLoads, connectorsToAssign);
        }
        
        // Build incremental connector assignments (newly assigned only)
        for (WorkerLoad wl : workerLoads) {
            List<String> newConnectors = new ArrayList<>(wl.connectors());
            newConnectors.removeAll(memberAssignments.getOrDefault(wl.worker(), ConnectorsAndTasks.EMPTY).connectors());
            if (!newConnectors.isEmpty()) {
                result.newlyAssigned.put(wl.worker(), newConnectors);
            }
        }
        
        // Build all connector assignments
        result.allAssigned = workerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));
        
        return result;
    }
    
    /**
     * Assign connectors to workers using round-robin (inherited from parent class).
     */
    @Override
    protected void assignConnectors(List<WorkerLoad> workerLoads, Collection<String> connectors) {
        // Sort workers by connector count (ascending) for round-robin
        workerLoads.sort(Comparator.comparingInt(w -> w.connectors().size()));
        
        List<String> sortedConnectors = new ArrayList<>(connectors);
        Collections.sort(sortedConnectors);
        
        int workerIndex = 0;
        for (String connector : sortedConnectors) {
            WorkerLoad worker = workerLoads.get(workerIndex);
            worker.connectors().add(connector);
            
            workerIndex = (workerIndex + 1) % workerLoads.size();
        }
    }
    
    /**
     * Identify deleted connectors and tasks that need to be revoked.
     */
    private DeletionInfo identifyDeletions(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<String> configuredConnectors,
            Set<ConnectorTaskId> configuredTasks) {
        
        DeletionInfo info = new DeletionInfo();
        Set<String> deletedConnectorNames = new HashSet<>();
        
        // Identify deleted connectors
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            Collection<String> existingConnectors = entry.getValue().connectors();
            
            List<String> deletedConnectors = existingConnectors.stream()
                    .filter(c -> !configuredConnectors.contains(c))
                    .collect(Collectors.toList());
            
            if (!deletedConnectors.isEmpty()) {
                info.connectorRevocations.put(worker, deletedConnectors);
                deletedConnectorNames.addAll(deletedConnectors);
                log.info("Revoking deleted connectors {} from worker {}", deletedConnectors, worker);
            }
        }
        
        // Revoke ALL tasks belonging to deleted connectors from ALL workers
        if (!deletedConnectorNames.isEmpty()) {
            for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
                String worker = entry.getKey();
                Collection<ConnectorTaskId> existingTasks = entry.getValue().tasks();
                
                List<ConnectorTaskId> deletedConnectorTasks = existingTasks.stream()
                        .filter(task -> deletedConnectorNames.contains(task.connector()))
                        .collect(Collectors.toList());
                
                if (!deletedConnectorTasks.isEmpty()) {
                    info.taskRevocations.put(worker, deletedConnectorTasks);
                    log.info("Revoking {} tasks from deleted connectors on worker {}",
                            deletedConnectorTasks.size(), worker);
                }
            }
        }
        
        return info;
    }
    
    /**
     * Merge two maps of collections.
     */
    private <K, V> Map<K, Collection<V>> mergeMaps(
            Map<K, Collection<V>> map1,
            Map<K, Collection<V>> map2) {
        
        Map<K, Collection<V>> result = new HashMap<>(map1);
        
        for (Map.Entry<K, Collection<V>> entry : map2.entrySet()) {
            result.merge(entry.getKey(), entry.getValue(), (existing, newVals) -> {
                List<V> combined = new ArrayList<>(existing);
                combined.addAll(newVals);
                return combined;
            });
        }
        
        return result;
    }
    
    // ==================== Inner Classes ====================
    /**
     * Worker state tracking assigned tasks and connectors.
     */
    static class WorkerState {
        final String worker;
        final Set<ConnectorTaskId> assignedTasks = new LinkedHashSet<>();
        
        WorkerState(String worker) {
            this.worker = worker;
        }
        
        int getTaskCountForConnector(String connector) {
            return (int) assignedTasks.stream()
                    .filter(t -> t.connector().equals(connector))
                    .count();
        }
    }
    
    /**
     * Balance targets for global and per-consumer distribution.
     */
    static class BalanceTargets {
        int totalTasks;
        int globalMin;
        int globalMax;
        int globalMaxLimit;
        final Map<String, ConsumerTarget> perConsumerTargets = new TreeMap<>();
    }
    
    /**
     * Per-consumer (connector) balance targets.
     */
    static class ConsumerTarget {
        int totalTasks;
        int consumerNTaskCountMin;
        int consumerNTaskCountMax;
    }
    
    /**
     * Connector assignment results.
     */
    static class ConnectorAssignments {
        final Map<String, Collection<String>> newlyAssigned = new HashMap<>();
        Map<String, Collection<String>> allAssigned = new HashMap<>();
    }
    
    /**
     * Information about deleted connectors and tasks.
     */
    static class DeletionInfo {
        final Map<String, Collection<String>> connectorRevocations = new HashMap<>();
        final Map<String, Collection<ConnectorTaskId>> taskRevocations = new HashMap<>();
    }
}

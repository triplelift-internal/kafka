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
        
        // Build current state from ConfigSnapshot
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
        
        // ═══════════════════════════════════════════════════════════════
        // STEP 1 (ALWAYS): Calculate Balance Targets
        // ═══════════════════════════════════════════════════════════════
        BalanceTargets targets = calculateBalanceTargets(workers, configuredConnectors, configuredTasks);
        
        // Identify unassigned tasks
        Set<ConnectorTaskId> allAssignedTasks = workers.stream()
                .flatMap(w -> w.assignedTasks.stream())
                .collect(Collectors.toSet());
        Set<ConnectorTaskId> unassignedTasks = new TreeSet<>(configuredTasks);
        unassignedTasks.removeAll(allAssignedTasks);
        
        log.info("Current state - Assigned: {}, Unassigned: {}", allAssignedTasks.size(), unassignedTasks.size());
        
        // ═══════════════════════════════════════════════════════════════
        // STEP 2 (ALWAYS): Check for Balance Violations
        // ═══════════════════════════════════════════════════════════════
        ViolationState violations = detectViolations(workers, targets, unassignedTasks);
        
        Map<String, Collection<ConnectorTaskId>> tasksToRevoke = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> tasksToAssign = new HashMap<>();
        
        // Cooperative Protocol Rule: A single generation can EITHER revoke OR assign, not both
        
        // ═══════════════════════════════════════════════════════════════
        // STEP 3 (CONDITIONAL): REVOCATION GENERATION
        // Only if overload violations detected
        // ═══════════════════════════════════════════════════════════════
        if (violations.hasOverloadViolations()) {
            log.info("=== STEP 3: REVOCATION GENERATION (overload violations detected) ===");
            tasksToRevoke = performRevocationGeneration(workers, violations, targets);
            
            log.info("Revocation complete - Revoked: {} tasks from {} workers", 
                    tasksToRevoke.values().stream().mapToInt(Collection::size).sum(),
                    tasksToRevoke.size());
            
            // Cooperative protocol: After revocations, we cannot assign in the same generation
            // Workers will rejoin and next generation will re-run Steps 1-2
            
        // ═══════════════════════════════════════════════════════════════
        // STEP 4 (CONDITIONAL): ASSIGNMENT GENERATION
        // Only if no overload violations AND (underload violations OR unassigned tasks)
        // ═══════════════════════════════════════════════════════════════
        } else if (violations.hasUnderloadViolations() || !unassignedTasks.isEmpty()) {
            log.info("=== STEP 4: ASSIGNMENT GENERATION (no overload, but underload or unassigned) ===");
            tasksToAssign = performAssignmentGeneration(workers, violations, targets, unassignedTasks);
            
            log.info("Assignment complete - Assigned: {} tasks to {} workers",
                    tasksToAssign.values().stream().mapToInt(Collection::size).sum(),
                    tasksToAssign.size());
        } else {
            log.info("Cluster is balanced, no actions needed");
        }
        
        // Build final task assignments
        Map<String, Collection<ConnectorTaskId>> allTaskAssignments = buildFinalTaskAssignments(
                workers, memberAssignments, tasksToRevoke, tasksToAssign, configuredConnectors);
        
        // Handle connector assignments
        ConnectorAssignments connectorAssignments = buildConnectorAssignments(
                workers, memberAssignments, allTaskAssignments, configuredConnectors);
        
        // Handle deleted connectors and tasks
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
    
    // ═══════════════════════════════════════════════════════════════
    // STEP 2: Detect Balance Violations
    // ═══════════════════════════════════════════════════════════════
    
    /**
     * Detect all types of balance violations.
     * Returns a ViolationState containing all detected violations.
     */
    private ViolationState detectViolations(
            List<WorkerState> workers,
            BalanceTargets targets,
            Set<ConnectorTaskId> unassignedTasks) {
        
        ViolationState violations = new ViolationState();
        
        log.debug("Detecting violations - targets: globalMin={}, globalMax={}, globalMaxLimit={}",
                targets.minTasksPerWorker, targets.maxTasksPerWorker, targets.maxTasksPerWorkerMaxLimit);
        
        // A. Global Balance Violation Check
        for (WorkerState worker : workers) {
            int totalTasks = worker.assignedTasks.size();
            
            if (totalTasks > targets.maxTasksPerWorkerMaxLimit) {
                violations.overloadedWorkers.add(worker);
                log.debug("Global OVERLOAD: worker {} has {} tasks > globalMaxLimit {}", 
                        worker.worker, totalTasks, targets.maxTasksPerWorkerMaxLimit);
            } else if (totalTasks < targets.minTasksPerWorker) {
                violations.underloadedWorkers.add(worker);
                log.debug("Global UNDERLOAD: worker {} has {} tasks < globalMin {}", 
                        worker.worker, totalTasks, targets.minTasksPerWorker);
            }
        }
        
        // B. Per-Consumer Balance Violation Check
        for (WorkerState worker : workers) {
            for (Map.Entry<String, ConsumerTarget> entry : targets.perConsumerTargets.entrySet()) {
                String consumer = entry.getKey();
                ConsumerTarget target = entry.getValue();
                int currentCount = worker.getTaskCountForConnector(consumer);
                
                if (currentCount > target.consumerNTaskCountMax) {
                    // Worker exceeds max for this consumer
                    violations.consumerOverloads.add(new ConsumerViolation(worker, consumer, currentCount, target));
                    log.debug("Per-consumer OVERLOAD: worker {} has {} tasks from {} > max {}", 
                            worker.worker, currentCount, consumer, target.consumerNTaskCountMax);
                } else if (currentCount == target.consumerNTaskCountMax && target.consumerNTaskCountMin < target.consumerNTaskCountMax) {
                    // Only check max count violations when min < max (imperfect division)
                    // When min == max (perfect division), all workers should have exactly that count
                    long workersWithMaxCount = workers.stream()
                            .filter(w -> w.getTaskCountForConnector(consumer) == target.consumerNTaskCountMax)
                            .count();
                    
                    if (workersWithMaxCount > target.maxWorkersWithTaskCountMax) {
                        // Too many workers have max - this worker needs to revoke to reach min
                        violations.consumerOverloads.add(new ConsumerViolation(worker, consumer, currentCount, target));
                        log.debug("Per-consumer MAX COUNT violation: {} workers have {} tasks from {} (max allowed: {})", 
                                workersWithMaxCount, target.consumerNTaskCountMax, consumer, target.maxWorkersWithTaskCountMax);
                    }
                } else if (currentCount < target.consumerNTaskCountMin) {
                    // Worker is below min for this consumer
                    violations.consumerUnderloads.add(new ConsumerViolation(worker, consumer, currentCount, target));
                    log.debug("Per-consumer UNDERLOAD: worker {} has {} tasks from {} < min {}", 
                            worker.worker, currentCount, consumer, target.consumerNTaskCountMin);
                }
            }
        }
        
        violations.unassignedTasks.addAll(unassignedTasks);
        
        log.info("Violation detection complete - Overloaded workers: {}, Consumer overloads: {}, " +
                "Underloaded workers: {}, Consumer underloads: {}, Unassigned: {}",
                violations.overloadedWorkers.size(), violations.consumerOverloads.size(),
                violations.underloadedWorkers.size(), violations.consumerUnderloads.size(),
                violations.unassignedTasks.size());
        
        // Debug: Log first few consumer violations for troubleshooting
        if (!violations.consumerOverloads.isEmpty()) {
            log.info("Sample consumer overloads: {}", violations.consumerOverloads.stream()
                    .limit(5)
                    .map(v -> String.format("%s/%s:has=%d,max=%d", v.worker.worker, v.consumer, v.actualCount, v.target.consumerNTaskCountMax))
                    .collect(Collectors.joining(", ")));
        }
        if (!violations.consumerUnderloads.isEmpty()) {
            log.info("Sample consumer underloads: {}", violations.consumerUnderloads.stream()
                    .limit(5)
                    .map(v -> String.format("%s/%s:has=%d,min=%d", v.worker.worker, v.consumer, v.actualCount, v.target.consumerNTaskCountMin))
                    .collect(Collectors.joining(", ")));
        }
        
        return violations;
    }
    
    // ═══════════════════════════════════════════════════════════════
    // STEP 3: Revocation Generation
    // ═══════════════════════════════════════════════════════════════
    
    /**
     * Perform incremental revocation from overloaded workers.
     * Revokes ALL excess tasks that violate limits in this generation.
     * Also performs "rebalancing revocations" when workers are below min but no unassigned tasks exist.
     */
    private Map<String, Collection<ConnectorTaskId>> performRevocationGeneration(
            List<WorkerState> workers,
            ViolationState violations,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        // 1. Revoke from per-consumer overloads FIRST
        // Sort by violation severity (most overloaded first)
        violations.consumerOverloads.sort((v1, v2) -> {
            int excess1 = v1.actualCount - v1.target.consumerNTaskCountMin;
            int excess2 = v2.actualCount - v2.target.consumerNTaskCountMin;
            return Integer.compare(excess2, excess1);
        });
        
        for (ConsumerViolation violation : violations.consumerOverloads) {
            WorkerState worker = violation.worker;
            String consumer = violation.consumer;
            
            // According to spec: Revoke down to consumerNTaskCountMin
            // excessCount = worker.consumerTaskCount[consumer] - perConsumerPerWorkerTaskCountMin[consumer]
            int currentCount = worker.getTaskCountForConnector(consumer);
            int excessCount = currentCount - violation.target.consumerNTaskCountMin;
            
            if (excessCount <= 0) {
                log.trace("Skipping consumer {} on worker {} - no excess after previous revocations (current: {}, min: {})",
                        consumer, worker.worker, currentCount, violation.target.consumerNTaskCountMin);
                continue;
            }
            
            log.debug("Revoking {} tasks of consumer {} from worker {} (current: {}, min: {})",
                    excessCount, consumer, worker.worker, currentCount, violation.target.consumerNTaskCountMin);
            
            // Revoke ALL excess tasks from this consumer on this worker
            List<ConnectorTaskId> tasksFromConsumer = worker.assignedTasks.stream()
                    .filter(task -> task.connector().equals(consumer))
                    .sorted()
                    .limit(excessCount)
                    .collect(Collectors.toList());
            
            for (ConnectorTaskId task : tasksFromConsumer) {
                revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(task);
                worker.assignedTasks.remove(task);
                
                log.trace("Revoked task {} from worker {} (per-consumer: {} now has {} tasks, min: {})",
                        task, worker.worker, consumer, worker.getTaskCountForConnector(consumer), 
                        violation.target.consumerNTaskCountMin);
            }
        }
        
        // 2. Revoke from globally overloaded workers
        // According to spec: Revoke while worker.totalTasks > perWorkerTotalTasksMin
        // IMPORTANT: Check current state (post per-consumer revocations), not original violations list
        for (WorkerState worker : workers) {
            // Only process workers that are STILL overloaded after per-consumer revocations
            if (worker.assignedTasks.size() <= targets.minTasksPerWorker) {
                continue;
            }
            
            log.debug("Worker {} globally overloaded after per-consumer revocations (has {} tasks, min: {})",
                    worker.worker, worker.assignedTasks.size(), targets.minTasksPerWorker);
            
            while (worker.assignedTasks.size() > targets.minTasksPerWorker) {
                // Select consumer with most tasks on this worker
                String consumerWithMost = findConsumerWithMostTasksOnWorker(worker);
                
                if (consumerWithMost == null) {
                    log.warn("Cannot find consumer to revoke from worker {} - breaking", worker.worker);
                    break;
                }
                
                // Revoke 1 task from that consumer
                ConnectorTaskId taskToRevoke = worker.assignedTasks.stream()
                        .filter(task -> task.connector().equals(consumerWithMost))
                        .sorted()
                        .findFirst()
                        .orElse(null);
                
                if (taskToRevoke == null) {
                    log.warn("Cannot find task to revoke from consumer {} on worker {}", 
                            consumerWithMost, worker.worker);
                    break;
                }
                
                revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(taskToRevoke);
                worker.assignedTasks.remove(taskToRevoke);
                
                log.trace("Revoked task {} from worker {} (global overload, now has {} tasks)",
                        taskToRevoke, worker.worker, worker.assignedTasks.size());
            }
        }
        
        // 3. Rebalancing revocations: If workers have per-consumer underloads but are at globalMax,
        // they need to drop tasks from over-represented connectors to make room
        for (ConsumerViolation underload : violations.consumerUnderloads) {
            WorkerState worker = underload.worker;
            
            // Only if worker is at or above globalMax (can't accept more tasks without dropping some)
            if (worker.assignedTasks.size() < targets.maxTasksPerWorker) {
                continue; // Worker has room, no need for rebalancing revocations
            }
            
            String underloadedConsumer = underload.consumer;
            int deficit = underload.target.consumerNTaskCountMin - underload.actualCount;
            
            if (deficit <= 0) {
                continue; // Already satisfied
            }
            
            log.debug("Rebalancing revocation: worker {} is underloaded for {} (has {}, needs {}+) but at globalMax",
                    worker.worker, underloadedConsumer, underload.actualCount, underload.target.consumerNTaskCountMin);
            
            // Find connectors where this worker has MORE than min (candidates for revocation)
            List<String> overRepresentedConnectors = targets.perConsumerTargets.entrySet().stream()
                    .filter(e -> {
                        String connector = e.getKey();
                        if (connector.equals(underloadedConsumer)) {
                            return false; // Don't revoke from the underloaded connector
                        }
                        int currentCount = worker.getTaskCountForConnector(connector);
                        int min = e.getValue().consumerNTaskCountMin;
                        return currentCount > min; // Has more than min
                    })
                    .sorted((e1, e2) -> {
                        // Sort by excess (most excess first)
                        int excess1 = worker.getTaskCountForConnector(e1.getKey()) - e1.getValue().consumerNTaskCountMin;
                        int excess2 = worker.getTaskCountForConnector(e2.getKey()) - e2.getValue().consumerNTaskCountMin;
                        return Integer.compare(excess2, excess1);
                    })
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
            
            // Revoke tasks from over-represented connectors to make room
            int tasksToRevoke = Math.min(deficit, overRepresentedConnectors.size());
            for (int i = 0; i < tasksToRevoke && i < overRepresentedConnectors.size(); i++) {
                String connectorToRevoke = overRepresentedConnectors.get(i);
                
                ConnectorTaskId taskToRevoke = worker.assignedTasks.stream()
                        .filter(task -> task.connector().equals(connectorToRevoke))
                        .sorted()
                        .findFirst()
                        .orElse(null);
                
                if (taskToRevoke != null) {
                    revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(taskToRevoke);
                    worker.assignedTasks.remove(taskToRevoke);
                    
                    log.debug("Rebalancing revocation: revoked task {} from worker {} to make room for {}",
                            taskToRevoke, worker.worker, underloadedConsumer);
                }
            }
        }
        
        return revocations;
    }
    
    // ═══════════════════════════════════════════════════════════════
    // STEP 4: Assignment Generation
    // ═══════════════════════════════════════════════════════════════
    
    /**
     * Perform incremental assignment to underloaded workers and unassigned tasks.
     * Consists of two phases:
     * - Phase A: Fill workers to minimum targets
     * - Phase B: Distribute remaining unassigned tasks
     */
    private Map<String, Collection<ConnectorTaskId>> performAssignmentGeneration(
            List<WorkerState> workers,
            ViolationState violations,
            BalanceTargets targets,
            Set<ConnectorTaskId> unassignedTasks) {
        
        Map<String, Collection<ConnectorTaskId>> assignments = new HashMap<>();
        
        // Group unassigned tasks by connector
        Map<String, List<ConnectorTaskId>> unassignedByConsumer = new TreeMap<>();
        for (ConnectorTaskId task : unassignedTasks) {
            unassignedByConsumer.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }
        
        // Sort tasks for determinism
        unassignedByConsumer.values().forEach(Collections::sort);
        
        log.debug("Assignment generation - {} unassigned tasks across {} consumers",
                unassignedTasks.size(), unassignedByConsumer.size());
        
        // PHASE A: Fill workers to minimum targets
        performPhaseA_FillToMinimum(workers, violations, targets, unassignedByConsumer, assignments);
        
        // PHASE B: Distribute remaining unassigned tasks
        performPhaseB_DistributeRemaining(workers, targets, unassignedByConsumer, assignments);
        
        return assignments;
    }
    
    /**
     * Phase A: Fill all workers to consumerNTaskCountMin for each consumer.
     * This ensures every worker has the minimum task count for every consumer.
     * Global balance limits are IGNORED in this phase - per-consumer balance is priority.
     */
    private void performPhaseA_FillToMinimum(
            List<WorkerState> workers,
            ViolationState violations,
            BalanceTargets targets,
            Map<String, List<ConnectorTaskId>> unassignedByConsumer,
            Map<String, Collection<ConnectorTaskId>> assignments) {
        
        log.debug("Phase A: Fill all workers to consumerNTaskCountMin for each consumer (ignoring global limits)");
        
        // Sort consumers by total task count (descending) to process largest first
        List<String> sortedConsumers = targets.perConsumerTargets.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e2.getValue().totalTasks, e1.getValue().totalTasks))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        // For each consumer, fill ALL workers to consumerNTaskCountMin
        for (String consumer : sortedConsumers) {
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            List<ConnectorTaskId> available = unassignedByConsumer.get(consumer);
            
            if (available == null || available.isEmpty()) {
                log.trace("Phase A - No unassigned tasks for consumer {}", consumer);
                continue;
            }
            
            log.debug("Phase A - Processing consumer {} with {} unassigned tasks, target min={} per worker",
                    consumer, available.size(), target.consumerNTaskCountMin);
            
            // Fill each worker to consumerNTaskCountMin for this consumer
            for (WorkerState worker : workers) {
                int currentCount = worker.getTaskCountForConnector(consumer);
                int deficit = target.consumerNTaskCountMin - currentCount;
                
                if (deficit <= 0) {
                    log.trace("Phase A - Worker {} already has min for consumer {} (has {}, min={})",
                            worker.worker, consumer, currentCount, target.consumerNTaskCountMin);
                    continue; // Worker already has minimum for this consumer
                }
                
                int tasksToAssign = Math.min(deficit, available.size());
                
                log.debug("Phase A - Assigning {} tasks to worker {} for consumer {} (current={}, min={}, total={})",
                        tasksToAssign, worker.worker, consumer, currentCount, target.consumerNTaskCountMin,
                        worker.assignedTasks.size());
                
                for (int i = 0; i < tasksToAssign; i++) {
                    ConnectorTaskId task = available.remove(0);
                    assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(task);
                    worker.assignedTasks.add(task);
                    
                    log.trace("Phase A - Assigned task {} to worker {} (consumer {} deficit, now has {})",
                            task, worker.worker, consumer, worker.getTaskCountForConnector(consumer));
                }
                
                if (available.isEmpty()) {
                    break; // No more tasks for this consumer
                }
            }
        }
        
        int remaining = unassignedByConsumer.values().stream().mapToInt(List::size).sum();
        log.debug("Phase A complete - Remaining unassigned: {}", remaining);
    }
    
    /**
     * Phase B: Round-robin distribution of remaining tasks.
     * Strategy: Process consumers sorted by task count (largest first).
     * For each consumer, assign 1 task at a time to workers in round-robin fashion.
     * Global limits are IGNORED - only per-consumer balance (consumerNTaskCountMax) is enforced.
     */
    private void performPhaseB_DistributeRemaining(
            List<WorkerState> workers,
            BalanceTargets targets,
            Map<String, List<ConnectorTaskId>> unassignedByConsumer,
            Map<String, Collection<ConnectorTaskId>> assignments) {
        
        log.debug("Phase B: Round-robin distribution of remaining tasks (ignoring global limits)");
        
        // Sort consumers by total task count (descending) - process largest first
        List<String> sortedConsumers = targets.perConsumerTargets.entrySet().stream()
                .filter(e -> {
                    List<ConnectorTaskId> tasks = unassignedByConsumer.get(e.getKey());
                    return tasks != null && !tasks.isEmpty();
                })
                .sorted((e1, e2) -> Integer.compare(e2.getValue().totalTasks, e1.getValue().totalTasks))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        log.debug("Phase B - Processing {} consumers with unassigned tasks", sortedConsumers.size());
        
        // Track which worker to start with for each consumer (for round-robin continuity)
        int workerStartIndex = 0;
        
        // Process each consumer in order
        for (String consumer : sortedConsumers) {
            List<ConnectorTaskId> unassigned = unassignedByConsumer.get(consumer);
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            
            if (unassigned.isEmpty()) {
                continue;
            }
            
            log.debug("Phase B - Processing consumer {} with {} unassigned tasks, max={} per worker",
                    consumer, unassigned.size(), target.consumerNTaskCountMax);
            
            // Round-robin assignment: 1 task at a time to each worker
            int currentWorkerIndex = workerStartIndex;
            
            while (!unassigned.isEmpty()) {
                boolean taskAssigned = false;
                int attemptsCount = 0;
                
                // Try to find a worker starting from currentWorkerIndex
                while (attemptsCount < workers.size()) {
                    WorkerState worker = workers.get(currentWorkerIndex);
                    int currentCount = worker.getTaskCountForConnector(consumer);
                    
                    // Check if this worker can accept another task for this consumer
                    // Only constraint: must not exceed consumerNTaskCountMax
                    if (currentCount < target.consumerNTaskCountMax) {
                        // Assign 1 task to this worker
                        ConnectorTaskId task = unassigned.remove(0);
                        assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).add(task);
                        worker.assignedTasks.add(task);
                        
                        log.trace("Phase B - Assigned task {} to worker {} (consumer {}, now has {}/{}, total: {})",
                                task, worker.worker, consumer, 
                                worker.getTaskCountForConnector(consumer), target.consumerNTaskCountMax,
                                worker.assignedTasks.size());
                        
                        taskAssigned = true;
                        
                        // Move to next worker for round-robin
                        currentWorkerIndex = (currentWorkerIndex + 1) % workers.size();
                        break;
                    }
                    
                    // This worker is at max for this consumer, try next worker
                    currentWorkerIndex = (currentWorkerIndex + 1) % workers.size();
                    attemptsCount++;
                }
                
                if (!taskAssigned) {
                    // All workers are at consumerNTaskCountMax for this consumer
                    // This should theoretically not happen if our math is correct, but handle gracefully
                    log.error("Phase B - Cannot assign remaining {} tasks for consumer {} - all workers at max={}",
                            unassigned.size(), consumer, target.consumerNTaskCountMax);
                    break;
                }
            }
            
            // Update start index for next consumer (maintain round-robin across consumers)
            workerStartIndex = currentWorkerIndex;
            
            log.debug("Phase B - Completed consumer {}, {} tasks remaining across all consumers",
                    consumer, unassignedByConsumer.values().stream().mapToInt(List::size).sum());
        }
        
        int remaining = unassignedByConsumer.values().stream().mapToInt(List::size).sum();
        log.debug("Phase B complete - Remaining unassigned: {}", remaining);
    }
    
    // Helper methods for revocation and assignment
    
    private String findConsumerWithMostTasksOnWorker(WorkerState worker) {
        Map<String, Long> taskCounts = worker.assignedTasks.stream()
                .collect(Collectors.groupingBy(ConnectorTaskId::connector, Collectors.counting()));
        
        return taskCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
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
            
            // Filter out tasks that no longer exist in the configuration
            // This handles both deleted connectors AND decreased task counts
            List<ConnectorTaskId> validTasks = entry.getValue().tasks().stream()
                    .filter(task -> configuredTasks.contains(task))
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
        targets.minTasksPerWorker = targets.totalTasks / numWorkers;
        targets.maxTasksPerWorker = (targets.totalTasks + numWorkers - 1) / numWorkers;  // ceiling
        targets.maxTasksPerWorkerMaxLimit = targets.maxTasksPerWorker + 3;  // Relaxed limit to prioritize per-consumer balance
        
        log.debug("Global targets - totalTasks: {}, globalMin: {}, globalMax: {}, globalMaxLimit: {}",
                targets.totalTasks, targets.minTasksPerWorker, targets.maxTasksPerWorker, targets.maxTasksPerWorkerMaxLimit);
        
        // Per-consumer targets
        Map<String, Integer> taskCountsByConnector = new TreeMap<>();
        for (ConnectorTaskId task : configuredTasks) {
            taskCountsByConnector.merge(task.connector(), 1, Integer::sum);
        }
        
        for (String connector : configuredConnectors) {
            int taskCount = taskCountsByConnector.getOrDefault(connector, 0);
            ConsumerTarget target = new ConsumerTarget();
            target.totalTasks = taskCount;
            target.consumerNTaskCountMin = taskCount / numWorkers;  // floor
            target.consumerNTaskCountMax = (taskCount + numWorkers - 1) / numWorkers;  // ceiling
            // maxWorkersWithMax = taskCount - (consumerNTaskCountMin × numWorkers)
            // This is the maximum number of workers that should have consumerNTaskCountMax tasks
            target.maxWorkersWithTaskCountMax = taskCount - (target.consumerNTaskCountMin * numWorkers);
            targets.perConsumerTargets.put(connector, target);
            
            log.debug("Consumer targets - {}: totalTasks={}, consumerNTaskCountMin={}, consumerNTaskCountMax={}, maxWorkersWithMax={}",
                    connector, target.totalTasks, target.consumerNTaskCountMin, target.consumerNTaskCountMax, target.maxWorkersWithTaskCountMax);
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
     * This includes:
     * 1. Tasks from deleted connectors (connector no longer exists)
     * 2. Tasks with IDs that no longer exist (task count decreased)
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
        
        // Revoke ALL tasks belonging to deleted connectors OR with non-existent task IDs
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            Collection<ConnectorTaskId> existingTasks = entry.getValue().tasks();
            
            // Find tasks that should be revoked:
            // 1. Tasks from deleted connectors
            // 2. Tasks whose IDs no longer exist (task count decreased)
            List<ConnectorTaskId> tasksToRevoke = existingTasks.stream()
                    .filter(task -> deletedConnectorNames.contains(task.connector()) 
                                    || !configuredTasks.contains(task))
                    .collect(Collectors.toList());
            
            if (!tasksToRevoke.isEmpty()) {
                info.taskRevocations.put(worker, tasksToRevoke);
                log.info("Revoking {} tasks from worker {} (deleted connectors or decreased task count)",
                        tasksToRevoke.size(), worker);
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
        int minTasksPerWorker;
        int maxTasksPerWorker;
        int maxTasksPerWorkerMaxLimit;
        final Map<String, ConsumerTarget> perConsumerTargets = new TreeMap<>();
    }
    
    /**
     * Per-consumer (connector) balance targets.
     */
    static class ConsumerTarget {
        int totalTasks;
        int consumerNTaskCountMin;
        int consumerNTaskCountMax;
        int maxWorkersWithTaskCountMax;  // Maximum number of workers that should have consumerNTaskCountMax tasks
    }
    
    /**
     * Tracks all detected balance violations.
     */
    static class ViolationState {
        List<WorkerState> overloadedWorkers = new ArrayList<>();
        List<WorkerState> underloadedWorkers = new ArrayList<>();
        List<ConsumerViolation> consumerOverloads = new ArrayList<>();
        List<ConsumerViolation> consumerUnderloads = new ArrayList<>();
        Set<ConnectorTaskId> unassignedTasks = new TreeSet<>();
        
        boolean hasOverloadViolations() {
            return !overloadedWorkers.isEmpty() || !consumerOverloads.isEmpty();
        }
        
        boolean hasUnderloadViolations() {
            return !underloadedWorkers.isEmpty() || !consumerUnderloads.isEmpty();
        }
    }
    
    /**
     * Represents a per-consumer violation for a specific worker.
     */
    static class ConsumerViolation {
        WorkerState worker;
        String consumer;
        int actualCount;
        ConsumerTarget target;
        
        ConsumerViolation(WorkerState worker, String consumer, int actualCount, ConsumerTarget target) {
            this.worker = worker;
            this.consumer = consumer;
            this.actualCount = actualCount;
            this.target = target;
        }
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

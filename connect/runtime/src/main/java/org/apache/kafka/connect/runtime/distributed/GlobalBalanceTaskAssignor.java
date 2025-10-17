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
import java.util.stream.Collectors;


/**
 * An advanced assignor that extends IncrementalCooperativeAssignor to provide both
 * per-consumer balance and global balance through multi-round cooperative rebalancing.
 * 
 * <p>Balance Constraints:
 * <ul>
 *   <li>Per-Consumer Balance: Task count difference ≤ 1 for each consumer across all workers</li>
 *   <li>Global Balance: Total task count difference ≤ 1 across all workers</li>
 * </ul>
 * 
 * <p>Algorithm:
 * <ul>
 *   <li>Round N (Revocation): Revoke tasks that violate per-consumer max constraints</li>
 *   <li>Round N+1 (Assignment Phase 1): Assign tasks to meet per-consumer min constraints</li>
 *   <li>Round N+2 (Assignment Phase 2): Round-robin remaining tasks to meet global balance</li>
 * </ul>
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
    protected ClusterAssignment performTaskAssignment(
            ClusterConfigState configSnapshot,
            int lastCompletedGenerationId,
            int currentGenerationId,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        this.configSnapshot = configSnapshot;
        
        log.info("Starting GlobalBalanceTaskAssignor for generation {} (previous: {})", 
                currentGenerationId, lastCompletedGenerationId);
        
        // First, let parent handle standard cooperative rebalancing (deletions, duplicates, lost assignments)
        ClusterAssignment baseAssignment = super.performTaskAssignment(
                configSnapshot,
                lastCompletedGenerationId,
                currentGenerationId,
                memberAssignments
        );
        
        // Skip our balancing if parent has pending operations or delayed rebalance is active
        if (delay > 0 || scheduledRebalance > 0) {
            log.info("Skipping global balance - delayed rebalance active (delay: {}, scheduled: {})", 
                    delay, scheduledRebalance);
            return baseAssignment;
        }
        
        boolean hadRevocations = baseAssignment.newlyRevokedTasks().values().stream()
                .anyMatch(tasks -> !tasks.isEmpty());
        
        if (hadRevocations) {
            log.info("Base assignment has revocations - waiting for next round to apply global balance");
            return baseAssignment;
        }
        
        // Apply per-consumer + global balance
        return applyGlobalBalance(baseAssignment, configSnapshot, memberAssignments);
    }
    
    /**
     * Applies per-consumer and global balance through cooperative rebalancing.
     */
    private ClusterAssignment applyGlobalBalance(
            ClusterAssignment baseAssignment,
            ClusterConfigState configSnapshot,
            Map<String, ConnectorsAndTasks> memberAssignments
    ) {
        Map<String, Collection<ConnectorTaskId>> allTaskAssignments = 
                new HashMap<>(baseAssignment.allAssignedTasks());
        
        log.info("=== Starting applyGlobalBalance ===");
        log.info("Total workers: {}", allTaskAssignments.size());
        
        if (allTaskAssignments.isEmpty() || allTaskAssignments.values().stream().allMatch(Collection::isEmpty)) {
            log.debug("No tasks to balance - skipping");
            return baseAssignment;
        }
        
        // Analyze current distribution per consumer
        Map<String, Map<String, List<ConnectorTaskId>>> consumerTasksByWorker = 
                analyzePerConsumerDistribution(allTaskAssignments);
        
        // Calculate balance targets
        BalanceTargets targets = calculateBalanceTargets(
                consumerTasksByWorker, 
                allTaskAssignments.keySet()
        );
        
        log.info("Global targets - min: {}, max: {}", targets.globalMin, targets.globalMax);
        for (Map.Entry<String, ConsumerTarget> entry : targets.consumerTargets.entrySet()) {
            ConsumerTarget ct = entry.getValue();
            log.info("Consumer {} - totalTasks: {}, min: {}, max: {}", 
                    ct.consumer, ct.totalTasks, ct.min, ct.max);
        }
        
        // Check if already balanced
        if (isBalanced(allTaskAssignments, consumerTasksByWorker, targets)) {
            log.info("Already balanced - no action needed");
            return baseAssignment;
        }
        
        // Compute revocations needed to achieve balance
        Map<String, Collection<ConnectorTaskId>> revocations = 
                computeBalancedRevocations(allTaskAssignments, consumerTasksByWorker, targets);
        
        if (!revocations.isEmpty()) {
            log.info("Revoking {} tasks from {} workers for global balance", 
                    revocations.values().stream().mapToInt(Collection::size).sum(),
                    revocations.size());
            
            // Update assignments to reflect revocations
            Map<String, Collection<ConnectorTaskId>> updatedAssignments = new HashMap<>();
            for (Map.Entry<String, Collection<ConnectorTaskId>> entry : allTaskAssignments.entrySet()) {
                String worker = entry.getKey();
                List<ConnectorTaskId> tasks = new ArrayList<>(entry.getValue());
                tasks.removeAll(revocations.getOrDefault(worker, Collections.emptyList()));
                updatedAssignments.put(worker, tasks);
            }
            
            return new ClusterAssignment(
                    baseAssignment.newlyAssignedConnectors(),
                    baseAssignment.newlyAssignedTasks(),
                    baseAssignment.newlyRevokedConnectors(),
                    revocations,
                    baseAssignment.allAssignedConnectors(),
                    updatedAssignments
            );
        }
        
        // No revocations needed - compute assignments for unassigned tasks
        Collection<ConnectorTaskId> unassignedTasks = findUnassignedTasks(
                configSnapshot, 
                allTaskAssignments
        );
        
        log.info("Total unassigned tasks: {}", unassignedTasks.size());
        if (!unassignedTasks.isEmpty()) {
            Map<String, List<ConnectorTaskId>> unassignedByConsumer = unassignedTasks.stream()
                    .collect(Collectors.groupingBy(
                            task -> extractConsumerFromConnector(task.connector()),
                            TreeMap::new,
                            Collectors.toList()
                    ));
            for (Map.Entry<String, List<ConnectorTaskId>> entry : unassignedByConsumer.entrySet()) {
                log.info("  Consumer {} has {} unassigned tasks", entry.getKey(), entry.getValue().size());
            }
            
            Map<String, Collection<ConnectorTaskId>> newAssignments = 
                    computeBalancedAssignments(allTaskAssignments, unassignedTasks, targets);
            
            log.info("New assignments computed for {} workers", newAssignments.size());
            int totalAssigned = newAssignments.values().stream().mapToInt(Collection::size).sum();
            log.info("Total tasks being assigned: {}", totalAssigned);
            
            if (!newAssignments.isEmpty()) {
                log.info("Assigning {} tasks to {} workers for global balance",
                        newAssignments.values().stream().mapToInt(Collection::size).sum(),
                        newAssignments.size());
                
                // Merge new assignments with existing ones
                Map<String, Collection<ConnectorTaskId>> updatedAssignments = new HashMap<>(allTaskAssignments);
                for (Map.Entry<String, Collection<ConnectorTaskId>> entry : newAssignments.entrySet()) {
                    updatedAssignments.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .addAll(entry.getValue());
                }
                
                return new ClusterAssignment(
                        baseAssignment.newlyAssignedConnectors(),
                        newAssignments,
                        baseAssignment.newlyRevokedConnectors(),
                        baseAssignment.newlyRevokedTasks(),
                        baseAssignment.allAssignedConnectors(),
                        updatedAssignments
                );
            }
        }
        
        log.info("=== Completed applyGlobalBalance (no changes) ===");
        return baseAssignment;
    }
    
    /**
     * Analyzes the per-consumer distribution of tasks across workers.
     */
    private Map<String, Map<String, List<ConnectorTaskId>>> analyzePerConsumerDistribution(
            Map<String, Collection<ConnectorTaskId>> taskAssignments
    ) {
        Map<String, Map<String, List<ConnectorTaskId>>> result = new TreeMap<>();
        
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : taskAssignments.entrySet()) {
            String worker = entry.getKey();
            for (ConnectorTaskId task : entry.getValue()) {
                String consumer = extractConsumerFromConnector(task.connector());
                result.computeIfAbsent(consumer, k -> new TreeMap<>())
                      .computeIfAbsent(worker, k -> new ArrayList<>())
                      .add(task);
            }
        }
        
        return result;
    }
    
    /**
     * Calculates balance targets for all consumers and global balance.
     */
    private BalanceTargets calculateBalanceTargets(
            Map<String, Map<String, List<ConnectorTaskId>>> consumerTasksByWorker,
            Set<String> allWorkers
    ) {
        int numWorkers = allWorkers.size();
        Map<String, ConsumerTarget> consumerTargets = new HashMap<>();
        
        // Calculate total tasks across all consumers
        int totalTasks = consumerTasksByWorker.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToInt(List::size)
                .sum();
        
        // Global balance targets
        int globalMin = totalTasks / numWorkers;
        int globalMax = globalMin + (totalTasks % numWorkers > 0 ? 1 : 0);
        
        // Per-consumer balance targets
        for (Map.Entry<String, Map<String, List<ConnectorTaskId>>> entry : consumerTasksByWorker.entrySet()) {
            String consumer = entry.getKey();
            int consumerTotalTasks = entry.getValue().values().stream()
                    .mapToInt(List::size)
                    .sum();
            
            int consumerMin = consumerTotalTasks / numWorkers;
            int consumerMax = consumerMin + (consumerTotalTasks % numWorkers > 0 ? 1 : 0);
            
            consumerTargets.put(consumer, new ConsumerTarget(
                    consumer, 
                    consumerTotalTasks, 
                    consumerMin, 
                    consumerMax
            ));
        }
        
        return new BalanceTargets(globalMin, globalMax, consumerTargets);
    }
    
    /**
     * Checks if current assignment is balanced.
     */
    private boolean isBalanced(
            Map<String, Collection<ConnectorTaskId>> allTaskAssignments,
            Map<String, Map<String, List<ConnectorTaskId>>> consumerTasksByWorker,
            BalanceTargets targets
    ) {
        // Check global balance
        int globalMinActual = allTaskAssignments.values().stream()
                .mapToInt(Collection::size)
                .min()
                .orElse(0);
        int globalMaxActual = allTaskAssignments.values().stream()
                .mapToInt(Collection::size)
                .max()
                .orElse(0);
        
        if (globalMaxActual - globalMinActual > 1) {
            log.debug("Global imbalance: min={}, max={}, diff={}", 
                    globalMinActual, globalMaxActual, globalMaxActual - globalMinActual);
            return false;
        }
        
        // Check per-consumer balance
        for (Map.Entry<String, Map<String, List<ConnectorTaskId>>> entry : consumerTasksByWorker.entrySet()) {
            String consumer = entry.getKey();
            Map<String, List<ConnectorTaskId>> workerTasks = entry.getValue();
            
            int minTasks = allTaskAssignments.keySet().stream()
                    .mapToInt(w -> workerTasks.getOrDefault(w, Collections.emptyList()).size())
                    .min()
                    .orElse(0);
            int maxTasks = allTaskAssignments.keySet().stream()
                    .mapToInt(w -> workerTasks.getOrDefault(w, Collections.emptyList()).size())
                    .max()
                    .orElse(0);
            
            if (maxTasks - minTasks > 1) {
                log.debug("Consumer {} imbalance: min={}, max={}, diff={}", 
                        consumer, minTasks, maxTasks, maxTasks - minTasks);
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Computes revocations needed to achieve balance.
     * 
     * Algorithm:
     * 1. For each consumer, revoke tasks > consumerMax
     * 2. Revoke additional tasks if too many workers are at consumerMax
     */
    private Map<String, Collection<ConnectorTaskId>> computeBalancedRevocations(
            Map<String, Collection<ConnectorTaskId>> allTaskAssignments,
            Map<String, Map<String, List<ConnectorTaskId>>> consumerTasksByWorker,
            BalanceTargets targets
    ) {
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        // Sort workers by task count (descending) for deterministic revocation
        List<String> workersSortedDesc = allTaskAssignments.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        // Phase 1: Revoke tasks that violate per-consumer max constraint
        for (Map.Entry<String, Map<String, List<ConnectorTaskId>>> consumerEntry : consumerTasksByWorker.entrySet()) {
            String consumer = consumerEntry.getKey();
            Map<String, List<ConnectorTaskId>> workerTasks = consumerEntry.getValue();
            ConsumerTarget target = targets.consumerTargets.get(consumer);
            
            if (target == null) continue;
            
            for (String worker : workersSortedDesc) {
                List<ConnectorTaskId> tasks = workerTasks.getOrDefault(worker, Collections.emptyList());
                int currentCount = tasks.size();
                
                if (currentCount > target.max) {
                    int toRevoke = currentCount - target.max;
                    List<ConnectorTaskId> tasksToRevoke = new ArrayList<>(tasks.subList(0, toRevoke));
                    revocations.computeIfAbsent(worker, k -> new ArrayList<>()).addAll(tasksToRevoke);
                    
                    log.debug("Revoking {} tasks from worker {} for consumer {} (current: {}, max: {})",
                            toRevoke, worker, consumer, currentCount, target.max);
                }
            }
        }
        
        // Phase 2: Revoke additional tasks if too many workers are at consumerMax
        // This prevents situation where too many workers have max and others have less than min
        log.info("Phase 2: Checking for excess workers at consumerMax");
        for (Map.Entry<String, Map<String, List<ConnectorTaskId>>> consumerEntry : consumerTasksByWorker.entrySet()) {
            String consumer = consumerEntry.getKey();
            Map<String, List<ConnectorTaskId>> workerTasks = consumerEntry.getValue();
            ConsumerTarget target = targets.consumerTargets.get(consumer);
            
            if (target == null) continue;
            
            // Count workers at max after Phase 1 revocations
            int workersAtMax = 0;
            for (String worker : allTaskAssignments.keySet()) {
                List<ConnectorTaskId> tasks = workerTasks.getOrDefault(worker, Collections.emptyList());
                Collection<ConnectorTaskId> workerRevocations = revocations.getOrDefault(worker, Collections.emptyList());
                
                // Calculate remaining tasks after revocations
                long remainingCount = tasks.stream()
                        .filter(t -> !workerRevocations.contains(t))
                        .count();
                
                if (remainingCount == target.max) {
                    workersAtMax++;
                }
            }
            
            // Expected workers at max = total tasks - (workers × min)
            int expectedWorkersAtMax = target.totalTasks - (allTaskAssignments.size() * target.min);
            
            log.info("Consumer {}: workersAtMax={}, expectedWorkersAtMax={}", 
                    consumer, workersAtMax, expectedWorkersAtMax);
            
            if (workersAtMax > expectedWorkersAtMax) {
                int additionalRevocations = workersAtMax - expectedWorkersAtMax;
                log.info("Phase 2: Consumer {}: {} workers at max, expected {}. Revoking {} additional tasks",
                        consumer, workersAtMax, expectedWorkersAtMax, additionalRevocations);
                
                // Revoke from workers that are at max (prioritize most loaded globally)
                for (String worker : workersSortedDesc) {
                    if (additionalRevocations <= 0) break;
                    
                    List<ConnectorTaskId> tasks = workerTasks.getOrDefault(worker, Collections.emptyList());
                    Collection<ConnectorTaskId> workerRevocations = revocations.getOrDefault(worker, Collections.emptyList());
                    
                    List<ConnectorTaskId> remainingTasks = tasks.stream()
                            .filter(t -> !workerRevocations.contains(t))
                            .collect(Collectors.toList());
                    
                    if (remainingTasks.size() == target.max && !remainingTasks.isEmpty()) {
                        ConnectorTaskId taskToRevoke = remainingTasks.get(0);
                        revocations.computeIfAbsent(worker, k -> new ArrayList<>()).add(taskToRevoke);
                        additionalRevocations--;
                        
                        log.info("Phase 2: Additional revocation from worker {} for consumer {}", worker, consumer);
                    }
                }
            }
        }
        
        log.info("--- Revocations Complete: {} workers, {} total tasks ---", 
                revocations.size(), 
                revocations.values().stream().mapToInt(Collection::size).sum());
        
        return revocations;
    }
    
    /**
     * Computes assignments for unassigned tasks to achieve balance.
     * 
     * Algorithm:
     * 1. Sort workers by task count (ascending)
     * 2. Sort consumers by min requirement (descending)
     * 3. For each worker, assign tasks to meet consumer min requirements
     * 4. Round-robin assign remaining tasks up to global max
     */
    private Map<String, Collection<ConnectorTaskId>> computeBalancedAssignments(
            Map<String, Collection<ConnectorTaskId>> allTaskAssignments,
            Collection<ConnectorTaskId> unassignedTasks,
            BalanceTargets targets
    ) {
        Map<String, Collection<ConnectorTaskId>> newAssignments = new HashMap<>();
        
        log.info("--- Computing Assignments ---");
        log.info("Total unassigned tasks to distribute: {}", unassignedTasks.size());
        
        // Group unassigned tasks by consumer
        Map<String, List<ConnectorTaskId>> unassignedByConsumer = unassignedTasks.stream()
                .collect(Collectors.groupingBy(
                        task -> extractConsumerFromConnector(task.connector()),
                        TreeMap::new,
                        Collectors.toList()
                ));
        
        for (Map.Entry<String, List<ConnectorTaskId>> entry : unassignedByConsumer.entrySet()) {
            log.info("  Consumer {} has {} unassigned tasks", entry.getKey(), entry.getValue().size());
        }
        
        // Sort workers by current task count (ascending)
        List<String> workersSortedAsc = allTaskAssignments.entrySet().stream()
                .sorted(Comparator.comparingInt((Map.Entry<String, Collection<ConnectorTaskId>> e) -> e.getValue().size())
                        .thenComparing(Map.Entry::getKey))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        log.info("Workers sorted by task count (least to most):");
        
        // Sort consumers by min requirement (descending) for priority assignment
        List<String> consumersSorted = targets.consumerTargets.values().stream()
                .sorted(Comparator.comparingInt((ConsumerTarget t) -> t.min).reversed()
                        .thenComparing(t -> t.consumer))
                .map(t -> t.consumer)
                .collect(Collectors.toList());
        
        // Track current state during assignment
        Map<String, Integer> workerTaskCounts = new HashMap<>();
        Map<String, Map<String, Integer>> consumerCountsPerWorker = new HashMap<>();
        
        // Initialize worker task counts and consumer counts per worker
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : allTaskAssignments.entrySet()) {
            String worker = entry.getKey();
            Collection<ConnectorTaskId> tasks = entry.getValue();
            
            workerTaskCounts.put(worker, tasks.size());
            
            Map<String, Integer> consumerCounts = new HashMap<>();
            for (ConnectorTaskId task : tasks) {
                String consumer = extractConsumerFromConnector(task.connector());
                consumerCounts.merge(consumer, 1, Integer::sum);
            }
            consumerCountsPerWorker.put(worker, consumerCounts);
        }
        
        // Phase 1: Assign tasks to meet consumer min requirements
        log.info("Phase 1: Filling workers to consumer min requirements");
        int phase1Assignments = 0;
        for (String worker : workersSortedAsc) {
            Map<String, Integer> workerConsumerCounts = consumerCountsPerWorker.get(worker);
            int workerStartCount = workerTaskCounts.get(worker);
            
            for (String consumer : consumersSorted) {
                ConsumerTarget target = targets.consumerTargets.get(consumer);
                if (target == null) continue;
                
                int currentCount = workerConsumerCounts.getOrDefault(consumer, 0);
                int currentGlobalCount = workerTaskCounts.get(worker);
                List<ConnectorTaskId> availableTasks = unassignedByConsumer.get(consumer);
                
                if (availableTasks != null && currentCount < target.min) {
                    int needed = target.min - currentCount;
                    // Don't exceed global max while filling to consumer min
                    int capacity = targets.globalMax - currentGlobalCount;
                    int toAssign = Math.min(Math.min(needed, availableTasks.size()), capacity);
                    
                    if (toAssign > 0) {
                        log.info("Phase 1: Assigning {} tasks to worker {} for consumer {} (current: {}, min: {}, globalCapacity: {})",
                                toAssign, worker, consumer, currentCount, target.min, capacity);
                    }
                    
                    for (int i = 0; i < toAssign; i++) {
                        ConnectorTaskId task = availableTasks.remove(0);
                        newAssignments.computeIfAbsent(worker, k -> new ArrayList<>()).add(task);
                        workerConsumerCounts.merge(consumer, 1, Integer::sum);
                        workerTaskCounts.merge(worker, 1, Integer::sum);
                        phase1Assignments++;
                    }
                }
            }
            
            int workerEndCount = workerTaskCounts.get(worker);
            if (workerEndCount > workerStartCount) {
                log.info("Phase 1: Worker {} assigned {} tasks (now has {})", 
                        worker, workerEndCount - workerStartCount, workerEndCount);
            }
        }
        log.info("Phase 1 complete: {} tasks assigned", phase1Assignments);
        
        // Phase 2: Round-robin assign remaining tasks up to global max
        List<ConnectorTaskId> remainingTasks = unassignedByConsumer.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
        
        log.info("Phase 2: Round-robin assigning {} remaining tasks", remainingTasks.size());
        
        int workerIndex = 0;
        int phase2Assignments = 0;
        int skippedTasks = 0;
        for (ConnectorTaskId task : remainingTasks) {
            String consumer = extractConsumerFromConnector(task.connector());
            ConsumerTarget consumerTarget = targets.consumerTargets.get(consumer);
            
            // Find next worker that hasn't reached global max AND per-consumer max
            String worker = null;
            for (int i = 0; i < workersSortedAsc.size(); i++) {
                String candidate = workersSortedAsc.get((workerIndex + i) % workersSortedAsc.size());
                int currentGlobalCount = workerTaskCounts.get(candidate);
                int currentConsumerCount = consumerCountsPerWorker.get(candidate).getOrDefault(consumer, 0);
                
                // Check both global max and per-consumer max
                if (currentGlobalCount < targets.globalMax && 
                    (consumerTarget == null || currentConsumerCount < consumerTarget.max)) {
                    worker = candidate;
                    workerIndex = (workerIndex + i + 1) % workersSortedAsc.size();
                    break;
                }
            }
            
            if (worker == null) {
                log.warn("Phase 2: Cannot assign task {} (consumer: {}) - all workers at capacity", task, consumer);
                skippedTasks++;
                continue; // Skip this task, will be assigned in next round
            }
            
            newAssignments.computeIfAbsent(worker, k -> new ArrayList<>()).add(task);
            consumerCountsPerWorker.get(worker).merge(consumer, 1, Integer::sum);
            workerTaskCounts.merge(worker, 1, Integer::sum);
            phase2Assignments++;
        }
        
        log.info("Phase 2 complete: {} tasks assigned, {} tasks skipped", phase2Assignments, skippedTasks);
        log.info("--- Assignments Complete: {} workers, {} total tasks assigned ---", 
                newAssignments.size(),
                newAssignments.values().stream().mapToInt(Collection::size).sum());
        
        return newAssignments;
    }
    
    /**
     * Finds all unassigned tasks based on configuration.
     */
    private Collection<ConnectorTaskId> findUnassignedTasks(
            ClusterConfigState configSnapshot,
            Map<String, Collection<ConnectorTaskId>> allTaskAssignments
    ) {
        Set<ConnectorTaskId> assignedTasks = allTaskAssignments.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        
        Set<ConnectorTaskId> allTasks = new HashSet<>();
        for (String connector : configSnapshot.connectors()) {
            int maxTasks = configSnapshot.taskCount(connector);
            for (int i = 0; i < maxTasks; i++) {
                allTasks.add(new ConnectorTaskId(connector, i));
            }
        }
        
        allTasks.removeAll(assignedTasks);
        return allTasks;
    }
    
    /**
     * Extracts the consumer group name from a connector name.
     */
    protected String extractConsumerFromConnector(String connector) {
        if (connector == null || connector.isEmpty()) {
            return "unknown";
        }
        
        // Pattern: {ConsumerPrefix}-connector{number}
        int dashIndex = connector.indexOf('-');
        if (dashIndex > 0) {
            return connector.substring(0, dashIndex);
        }
        
        // Fallback to full connector name
        return connector;
    }
    
    /**
     * Balance targets for global and per-consumer balance.
     */
    private static class BalanceTargets {
        @SuppressWarnings("unused") // Reserved for future validation logic
        final int globalMin;
        final int globalMax;
        final Map<String, ConsumerTarget> consumerTargets;
        
        BalanceTargets(int globalMin, int globalMax, Map<String, ConsumerTarget> consumerTargets) {
            this.globalMin = globalMin;
            this.globalMax = globalMax;
            this.consumerTargets = consumerTargets;
        }
    }
    
    /**
     * Per-consumer balance target.
     */
    private static class ConsumerTarget {
        final String consumer;
        final int totalTasks;
        final int min;
        final int max;
        
        ConsumerTarget(String consumer, int totalTasks, int min, int max) {
            this.consumer = consumer;
            this.totalTasks = totalTasks;
            this.min = min;
            this.max = max;
        }
    }
}
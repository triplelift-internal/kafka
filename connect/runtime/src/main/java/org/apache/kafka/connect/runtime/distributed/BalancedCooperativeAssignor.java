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
 * An advanced assignor that extends IncrementalCooperativeAssignor to provide both
 * per-consumer balance and global balance through multi-round cooperative rebalancing.
 * 
 * <p>Balance Constraints:
 * <ul>
 *   <li>Per-Consumer Balance: Distributes tasks evenly for each consumer across all workers (max difference ≤ 1)</li>
 *   <li>Global Balance: Maintains total task count difference ≤ 2 across all workers (allows +1 tolerance for consumer balance)</li>
 *   <li>Balancing converges within a bounded number of rebalance rounds (≤6)</li>
 * </ul>
 * 
 * <p>Algorithm Phases:
 * <ul>
 *   <li>Phase 1: Revoke tasks to bring workers to cNmax for each consumer</li>
 *   <li>Phase 2: Assign unassigned tasks to fill workers to cNmin</li>
 *   <li>Phase 3: Revoke from workers at cNmax to create tasks for workers below cNmin</li>
 *   <li>Phase 4: Assign those revoked tasks to workers below cNmin</li>
 *   <li>Global: Balance total tasks across workers after per-consumer balance</li>
 * </ul>
 * 
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
        
        log.info("Starting BalancedCooperativeAssignor for generation {} (previous: {})", 
                currentGenerationId, lastCompletedGenerationId);
        
        // Get all configured connectors and tasks
        Set<String> configuredConnectors = new TreeSet<>(configSnapshot.connectors());
        Set<ConnectorTaskId> configuredTasks = new TreeSet<>();
        for (String connector : configuredConnectors) {
            configuredTasks.addAll(configSnapshot.tasks(connector));
        }
        
        log.debug("Configured connectors: {}, tasks: {}", configuredConnectors.size(), configuredTasks.size());
        
        // Detect lost assignments (from workers that left) - similar to parent's logic
        Set<String> activeMembers = new TreeSet<>(memberAssignments.keySet());
        Set<String> removedMembers = new TreeSet<>(previousMembers);
        removedMembers.removeAll(activeMembers);
        
        boolean workersLeft = !removedMembers.isEmpty();
        if (workersLeft) {
            log.info("Detected {} workers left the cluster: {}", removedMembers.size(), removedMembers);
            // If workers left and we don't have a scheduled rebalance, start the delay
            if (scheduledRebalance <= 0 && maxDelay > 0) {
                delay = maxDelay;
                scheduledRebalance = time.milliseconds() + delay;
                log.info("Workers left the cluster. Setting delayed rebalance: delay={}ms, scheduledRebalance={}", 
                        delay, scheduledRebalance);
                // Add all current workers as candidates for reassignment
                candidateWorkersForReassignment.addAll(activeMembers);
            }
        }
        
        // Check if delayed rebalance has expired
        long now = time.milliseconds();
        if (scheduledRebalance > 0 && now >= scheduledRebalance) {
            log.info("Delayed rebalance expired. Proceeding with rebalancing. scheduledRebalance={}, now={}", 
                    scheduledRebalance, now);
            resetDelay();
        } else if (scheduledRebalance > 0 && now < scheduledRebalance) {
            // Recalculate delay for next round
            delay = (int) (scheduledRebalance - now);
            log.info("Delayed rebalance still active. Remaining delay: {}ms", delay);
        }
        
        // Build worker state (filtering out tasks from deleted connectors)
        List<WorkerState> workers = buildWorkerState(memberAssignments, configuredTasks, configuredConnectors);
        
        // Calculate balance targets
        BalanceTargets targets = calculateBalanceTargets(workers, configuredConnectors, configuredTasks);
        
        // Analyze current balance state
        BalanceState state = analyzeBalance(workers, targets);
        
        log.info("Balance state - perConsumerBalanced: {}, globalBalanced: {}, hasUnassigned: {}, unassignedCount: {}, delay: {}", 
                state.perConsumerBalanced, state.globalBalanced, state.hasUnassignedTasks, state.unassignedTaskCount, delay);
        
        // Determine which phase to execute
        Map<String, Collection<ConnectorTaskId>> tasksToRevoke = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> tasksToAssign = new HashMap<>();
        
        // Calculate load-balancing revocations (if delay allows)
        Map<String, Collection<ConnectorTaskId>> loadBalancingRevocations = new HashMap<>();
        
        if (!state.perConsumerBalanced) {
            // Execute per-consumer balancing phases
            if (state.hasWorkersAboveCNmax) {
                log.info("Executing Phase 1: Revoke to cNmax");
                loadBalancingRevocations = performPhase1Revocation(workers, targets);
            } else if (state.hasWorkersBelowCNmin) {
                if (state.hasUnassignedTasks) {
                    log.info("Executing Phase 2: Assign to cNmin");
                    tasksToAssign = performPhase2Assignment(workers, targets);
                } else {
                    log.info("Executing Phase 3: Revoke from cNmax to fill cNmin");
                    loadBalancingRevocations = performPhase3Revocation(workers, targets);
                }
            }
        } else if (!state.globalBalanced) {
            // Execute global balancing
            if (state.hasWorkersAboveGlobalMaxLimit) {
                log.info("Executing Global Revocation");
                loadBalancingRevocations = performGlobalRevocation(workers, targets);
            } else if (state.hasUnassignedTasks) {
                log.info("Executing Global Assignment");
                tasksToAssign = performPhase2Assignment(workers, targets);
            }
        } else if (state.hasUnassignedTasks) {
            // All balanced but unassigned tasks remain
            log.info("Executing final assignment of remaining tasks");
            tasksToAssign = performPhase2Assignment(workers, targets);
        } else {
            log.info("Cluster is fully balanced!");
        }
        
        // Apply delayed rebalance logic (similar to parent IncrementalCooperativeAssignor)
        // Do not revoke resources for re-assignment while a delayed rebalance is active
        if (delay == 0) {
            // If this round and the previous round involved revocation, we will calculate a delay for
            // the next round when revoking rebalance would be allowed. Note that delay could be 0, in which
            // case we would always revoke.
            if (revokedInPrevious && !loadBalancingRevocations.isEmpty()) {
                numSuccessiveRevokingRebalances++;
                log.debug("Consecutive revoking rebalances observed. Computing delay and next scheduled rebalance.");
                delay = (int) consecutiveRevokingRebalancesBackoff.backoff(numSuccessiveRevokingRebalances);
                if (delay != 0) {
                    scheduledRebalance = time.milliseconds() + delay;
                    log.info("Skipping revocations in the current round with a delay of {}ms. Next scheduled rebalance: {}",
                            delay, scheduledRebalance);
                    // Skip revocations this round
                    loadBalancingRevocations.clear();
                } else {
                    log.debug("Revoking assignments immediately since scheduled.rebalance.max.delay.ms is set to 0");
                    tasksToRevoke = loadBalancingRevocations;
                }
            } else if (!loadBalancingRevocations.isEmpty()) {
                // We had a revocation in this round but not in the previous round. Let's store that state.
                log.debug("Performing allocation-balancing revocation immediately as no revocations took place during the previous rebalance");
                tasksToRevoke = loadBalancingRevocations;
                revokedInPrevious = true;
            } else if (revokedInPrevious) {
                // No revocations in this round but the previous round had one. Probably the workers
                // have converged to a balanced load. We can reset the rebalance clock
                log.debug("Previous round had revocations but this round didn't. Probably, the cluster has reached a " +
                        "balanced load. Resetting the exponential backoff clock");
                revokedInPrevious = false;
                numSuccessiveRevokingRebalances = 0;
            } else {
                // no-op
                log.debug("No revocations in previous and current round.");
            }
        } else {
            log.info("Delayed rebalance is active. Delaying {}ms before revoking connectors and tasks", delay);
            // Skip load-balancing revocations during delay
            loadBalancingRevocations.clear();
            revokedInPrevious = false;
        }
        
        // Build final assignments (target state after this round)
        // allTaskAssignments = current assignments - revoked + assigned
        // Also filter out tasks from deleted connectors
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
        
        // Connectors: Start with existing assignments, then assign unassigned connectors
        // First, build WorkerLoad with existing connector assignments
        List<WorkerLoad> workerLoads = new ArrayList<>();
        Set<String> alreadyAssignedConnectors = new HashSet<>();
        
        for (WorkerState worker : workers) {
            Collection<String> existingConnectors = memberAssignments
                    .getOrDefault(worker.worker, ConnectorsAndTasks.EMPTY)
                    .connectors();
            // Filter out connectors that no longer exist in the configuration
            List<String> validConnectors = existingConnectors.stream()
                    .filter(configuredConnectors::contains)
                    .collect(Collectors.toList());
            alreadyAssignedConnectors.addAll(validConnectors);
            
            workerLoads.add(new WorkerLoad.Builder(worker.worker)
                    .with(validConnectors, 
                          allTaskAssignments.getOrDefault(worker.worker, Collections.emptyList()))
                    .build());
        }
        
        // Determine which connectors need assignment (not already assigned to anyone)
        Set<String> connectorsToAssign = new TreeSet<>(configuredConnectors);
        connectorsToAssign.removeAll(alreadyAssignedConnectors);
        
        log.debug("Connectors already assigned: {}, to assign: {}", 
                alreadyAssignedConnectors.size(), connectorsToAssign.size());
        
        // Assign unassigned connectors using round-robin
        if (!connectorsToAssign.isEmpty()) {
            assignConnectors(workerLoads, connectorsToAssign);
        }
        
        // Build maps of incremental and total assignments
        Map<String, Collection<String>> incrementalConnectorAssignments = new HashMap<>();
        for (WorkerLoad wl : workerLoads) {
            List<String> newConnectors = new ArrayList<>(wl.connectors());
            newConnectors.removeAll(memberAssignments.getOrDefault(wl.worker(), ConnectorsAndTasks.EMPTY).connectors());
            if (!newConnectors.isEmpty()) {
                incrementalConnectorAssignments.put(wl.worker(), newConnectors);
            }
        }
        
        Map<String, Collection<String>> allConnectorAssignments = workerLoads.stream()
                .collect(Collectors.toMap(
                        WorkerLoad::worker,
                        WorkerLoad::connectors
                ));
        
        // Compute revoked connectors: connectors that exist in memberAssignments but not in configuredConnectors
        Map<String, Collection<String>> revokedConnectors = new HashMap<>();
        Set<String> deletedConnectorNames = new HashSet<>();
        for (WorkerState worker : workers) {
            Collection<String> existingConnectors = memberAssignments
                    .getOrDefault(worker.worker, ConnectorsAndTasks.EMPTY)
                    .connectors();
            List<String> deletedConnectors = existingConnectors.stream()
                    .filter(c -> !configuredConnectors.contains(c))
                    .collect(Collectors.toList());
            if (!deletedConnectors.isEmpty()) {
                revokedConnectors.put(worker.worker, deletedConnectors);
                deletedConnectorNames.addAll(deletedConnectors);
                log.info("Revoking deleted connectors {} from worker {}", deletedConnectors, worker.worker);
            }
        }
        
        // Revoke ALL tasks belonging to deleted connectors from ALL workers
        // (not just the worker that has the connector, since tasks can be on any worker)
        Map<String, Collection<ConnectorTaskId>> revokedConnectorTasks = new HashMap<>();
        if (!deletedConnectorNames.isEmpty()) {
            for (WorkerState worker : workers) {
                Collection<ConnectorTaskId> existingTasks = memberAssignments
                        .getOrDefault(worker.worker, ConnectorsAndTasks.EMPTY)
                        .tasks();
                List<ConnectorTaskId> deletedConnectorTasks = existingTasks.stream()
                        .filter(task -> deletedConnectorNames.contains(task.connector()))
                        .collect(Collectors.toList());
                if (!deletedConnectorTasks.isEmpty()) {
                    revokedConnectorTasks.put(worker.worker, deletedConnectorTasks);
                    log.info("Revoking {} tasks from deleted connectors on worker {}", deletedConnectorTasks.size(), worker.worker);
                }
            }
        }
        
        // Merge deleted connector tasks with other task revocations
        Map<String, Collection<ConnectorTaskId>> allTaskRevocations = new HashMap<>(tasksToRevoke);
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : revokedConnectorTasks.entrySet()) {
            allTaskRevocations.merge(entry.getKey(), entry.getValue(), (existing, newTasks) -> {
                List<ConnectorTaskId> combined = new ArrayList<>(existing);
                combined.addAll(newTasks);
                return combined;
            });
        }
        
        log.info("Assignment summary - Connector revocations: {}, Task revocations: {}, Task assignments: {}", 
                revokedConnectors.values().stream().mapToInt(Collection::size).sum(),
                allTaskRevocations.values().stream().mapToInt(Collection::size).sum(),
                tasksToAssign.values().stream().mapToInt(Collection::size).sum());
        
        // Update state for next round (similar to parent class)
        previousMembers = activeMembers;
        previousGenerationId = currentGenerationId;
        
        return new ClusterAssignment(
                incrementalConnectorAssignments,
                tasksToAssign,
                revokedConnectors,  // revoke deleted connectors
                allTaskRevocations,  // revoke deleted connector tasks + other revocations
                allConnectorAssignments,
                allTaskAssignments
        );
    }
    
    // ===== Phase 1: Revocation to cNmax =====
    
    private Map<String, Collection<ConnectorTaskId>> performPhase1Revocation(
            List<WorkerState> workers,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        for (String connector : targets.perConsumerTargets.keySet()) {
            ConsumerTarget target = targets.perConsumerTargets.get(connector);
            int cNmax = target.cNmax;
            
            log.debug("Phase 1 - Connector: {}, cNmax: {}", connector, cNmax);
            
            if (target.totalTasks > workers.size()) {
                // Strategy A: Consumers with tasks > workers
                for (WorkerState worker : workers) {
                    int currentCount = worker.getTaskCountForConnector(connector);
                    if (currentCount > cNmax) {
                        int toRevoke = currentCount - cNmax;
                        List<ConnectorTaskId> tasks = selectTasksForRevocation(worker, connector, toRevoke);
                        if (!tasks.isEmpty()) {
                            revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                            log.debug("Phase 1 - Revoking {} tasks from worker {} for connector {}", 
                                    tasks.size(), worker.worker, connector);
                        }
                    }
                }
            } else {
                // Strategy B: Consumers with tasks ≤ workers
                for (WorkerState worker : workers) {
                    int currentCount = worker.getTaskCountForConnector(connector);
                    if (currentCount > 1) {
                        int toRevoke = currentCount - 1;
                        List<ConnectorTaskId> tasks = selectTasksForRevocation(worker, connector, toRevoke);
                        if (!tasks.isEmpty()) {
                            revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                            log.debug("Phase 1 - Revoking {} tasks from worker {} for connector {} (Strategy B)", 
                                    tasks.size(), worker.worker, connector);
                        }
                    }
                }
            }
        }
        
        return revocations;
    }
    
    // ===== Phase 2: Assignment to cNmin =====
    
    private Map<String, Collection<ConnectorTaskId>> performPhase2Assignment(
            List<WorkerState> workers,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> assignments = new HashMap<>();
        
        // Get all unassigned tasks grouped by connector
        Map<String, List<ConnectorTaskId>> unassignedByConsumer = new TreeMap<>();
        for (WorkerState worker : workers) {
            for (ConnectorTaskId task : worker.unassignedTasks) {
                unassignedByConsumer.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
            }
        }
        
        // Sort tasks for determinism
        unassignedByConsumer.values().forEach(Collections::sort);
        
        // Sort workers by total load (ascending)
        List<WorkerState> sortedWorkers = new ArrayList<>(workers);
        
        // Sort consumers by cNmin (descending - prioritize high cNmin first)
        List<String> sortedConsumers = targets.perConsumerTargets.keySet().stream()
                .sorted((c1, c2) -> Integer.compare(
                        targets.perConsumerTargets.get(c2).cNmin,
                        targets.perConsumerTargets.get(c1).cNmin))
                .collect(Collectors.toList());
        
        // STEP 1: Fill each worker to cNmin for each consumer
        for (String consumer : sortedConsumers) {
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            List<ConnectorTaskId> unassigned = unassignedByConsumer.getOrDefault(consumer, new ArrayList<>());
            
            if (unassigned.isEmpty()) {
                continue;
            }
            
            log.debug("Phase 2 Step 1 - Consumer: {}, cNmin: {}, unassigned: {}", 
                    consumer, target.cNmin, unassigned.size());
            
            // Re-sort workers by current load before each consumer
            sortedWorkers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
            
            for (WorkerState worker : sortedWorkers) {
                int currentCount = worker.getTaskCountForConnector(consumer);
                if (currentCount < target.cNmin && !unassigned.isEmpty()) {
                    int needed = target.cNmin - currentCount;
                    int available = unassigned.size();
                    int toAssign = Math.min(needed, available);
                    
                    List<ConnectorTaskId> tasks = selectTasksForAssignment(unassigned, toAssign);
                    assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                    worker.assignedTasks.addAll(tasks);
                    unassigned.removeAll(tasks);
                    
                    log.debug("Phase 2 Step 1 - Assigned {} tasks to worker {} for connector {}", 
                            tasks.size(), worker.worker, consumer);
                }
            }
        }
        
        // STEP 2: Evenly distribute consumers with >2 unassigned tasks
        for (Map.Entry<String, List<ConnectorTaskId>> entry : new HashMap<>(unassignedByConsumer).entrySet()) {
            String consumer = entry.getKey();
            List<ConnectorTaskId> unassigned = entry.getValue();
            
            if (unassigned.size() <= 2) {
                continue;
            }
            
            log.debug("Phase 2 Step 2 - Consumer: {}, unassigned: {}", consumer, unassigned.size());
            
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            boolean isSmallConsumer = target != null && target.totalTasks <= workers.size();
            
            int tasksPerWorker = unassigned.size() / workers.size();
            int remainder = unassigned.size() % workers.size();
            
            // Re-sort by current load
            sortedWorkers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
            
            for (WorkerState worker : sortedWorkers) {
                if (unassigned.isEmpty()) {
                    break;
                }
                
                // For small consumers (tasks ≤ workers), enforce max 1 task per worker
                if (isSmallConsumer && worker.getTaskCountForConnector(consumer) >= 1) {
                    log.debug("Phase 2 Step 2 - Skipping worker {} for small consumer {} (already has 1 task)", 
                            worker.worker, consumer);
                    continue;
                }
                
                int allocation = tasksPerWorker + (remainder > 0 ? 1 : 0);
                int capacity = targets.globalMaxLimit - worker.assignedTasks.size();
                int toAssign = Math.min(Math.min(allocation, capacity), unassigned.size());
                
                // For small consumers, limit to 1 task per worker
                if (isSmallConsumer) {
                    toAssign = Math.min(toAssign, 1);
                }
                
                if (toAssign > 0) {
                    List<ConnectorTaskId> tasks = selectTasksForAssignment(unassigned, toAssign);
                    assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                    worker.assignedTasks.addAll(tasks);
                    unassigned.removeAll(tasks);
                    
                    if (remainder > 0) {
                        remainder--;
                    }
                    
                    log.debug("Phase 2 Step 2 - Assigned {} tasks to worker {} for connector {}", 
                            tasks.size(), worker.worker, consumer);
                }
            }
        }
        
        // STEP 3: Round-robin allocation to globalMax
        boolean progress = true;
        while (progress && unassignedByConsumer.values().stream().anyMatch(l -> !l.isEmpty())) {
            progress = false;
            sortedWorkers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
            
            for (WorkerState worker : sortedWorkers) {
                if (worker.assignedTasks.size() >= targets.globalMax) {
                    continue;
                }
                
                // Find consumer with most unassigned tasks that this worker can accept
                String consumer = findConsumerWithMostUnassignedForWorker(
                        unassignedByConsumer, worker, targets, workers.size());
                if (consumer == null) {
                    break;
                }
                
                List<ConnectorTaskId> unassigned = unassignedByConsumer.get(consumer);
                List<ConnectorTaskId> tasks = selectTasksForAssignment(unassigned, 1);
                
                if (!tasks.isEmpty()) {
                    assignments.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                    worker.assignedTasks.addAll(tasks);
                    unassigned.removeAll(tasks);
                    progress = true;
                    
                    log.debug("Phase 2 Step 3 - Assigned 1 task to worker {} for connector {}", 
                            worker.worker, consumer);
                }
            }
        }
        
        // STEP 4: Final allocation to globalMaxLimit
        for (Map.Entry<String, List<ConnectorTaskId>> entry : unassignedByConsumer.entrySet()) {
            String consumer = entry.getKey();
            List<ConnectorTaskId> unassigned = entry.getValue();
            
            ConsumerTarget target = targets.perConsumerTargets.get(consumer);
            boolean isSmallConsumer = target != null && target.totalTasks <= workers.size();
            
            while (!unassigned.isEmpty()) {
                sortedWorkers.sort(Comparator.comparingInt(w -> w.assignedTasks.size()));
                
                // Find a worker that can accept this task
                WorkerState selectedWorker = null;
                for (WorkerState worker : sortedWorkers) {
                    // Check global capacity
                    if (worker.assignedTasks.size() >= targets.globalMaxLimit) {
                        continue;
                    }
                    
                    // For small consumers, check if worker already has a task from this consumer
                    if (isSmallConsumer && worker.getTaskCountForConnector(consumer) >= 1) {
                        continue;
                    }
                    
                    selectedWorker = worker;
                    break;
                }
                
                if (selectedWorker == null) {
                    if (isSmallConsumer) {
                        log.warn("Cannot assign remaining {} tasks for small consumer {} - all workers either at globalMaxLimit or already have 1 task from this consumer", 
                                unassigned.size(), consumer);
                    } else {
                        log.error("Cannot assign remaining tasks - all workers at globalMaxLimit!");
                    }
                    break;
                }
                
                List<ConnectorTaskId> tasks = selectTasksForAssignment(unassigned, 1);
                assignments.computeIfAbsent(selectedWorker.worker, k -> new ArrayList<>()).addAll(tasks);
                selectedWorker.assignedTasks.addAll(tasks);
                unassigned.removeAll(tasks);
                
                log.debug("Phase 2 Step 4 - Assigned 1 task to worker {} (at globalMaxLimit tolerance)", 
                        selectedWorker.worker);
            }
        }
        
        return assignments;
    }
    
    // ===== Phase 3: Revocation from cNmax to fill cNmin =====
    
    private Map<String, Collection<ConnectorTaskId>> performPhase3Revocation(
            List<WorkerState> workers,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        for (String connector : targets.perConsumerTargets.keySet()) {
            ConsumerTarget target = targets.perConsumerTargets.get(connector);
            
            // Only process consumers with tasks > workers
            if (target.totalTasks <= workers.size()) {
                continue;
            }
            
            log.debug("Phase 3 - Connector: {}, cNmin: {}, cNmax: {}", 
                    connector, target.cNmin, target.cNmax);
            
            // Find workers below minimum
            List<WorkerState> workersBelowMin = new ArrayList<>();
            int totalMissing = 0;
            
            for (WorkerState worker : workers) {
                int currentCount = worker.getTaskCountForConnector(connector);
                if (currentCount < target.cNmin) {
                    workersBelowMin.add(worker);
                    totalMissing += target.cNmin - currentCount;
                }
            }
            
            if (workersBelowMin.isEmpty() || totalMissing == 0) {
                continue;
            }
            
            log.debug("Phase 3 - Workers below min: {}, total missing: {}", 
                    workersBelowMin.size(), totalMissing);
            
            // Find workers at maximum
            List<WorkerState> workersAtMax = new ArrayList<>();
            for (WorkerState worker : workers) {
                int currentCount = worker.getTaskCountForConnector(connector);
                if (currentCount == target.cNmax) {
                    workersAtMax.add(worker);
                }
            }
            
            // Fallback to workers at (cNmax - 1) if no workers at cNmax
            if (workersAtMax.isEmpty()) {
                for (WorkerState worker : workers) {
                    int currentCount = worker.getTaskCountForConnector(connector);
                    if (currentCount == target.cNmax - 1) {
                        workersAtMax.add(worker);
                    }
                }
            }
            
            if (workersAtMax.isEmpty()) {
                log.warn("Phase 3 - No workers at cNmax or (cNmax-1) for connector {}", connector);
                continue;
            }
            
            log.debug("Phase 3 - Workers at max: {}", workersAtMax.size());
            
            // Revoke exactly totalMissing tasks
            int revoked = 0;
            for (WorkerState worker : workersAtMax) {
                if (revoked >= totalMissing) {
                    break;
                }
                
                int toRevoke = Math.min(1, totalMissing - revoked);
                List<ConnectorTaskId> tasks = selectTasksForRevocation(worker, connector, toRevoke);
                
                if (!tasks.isEmpty()) {
                    revocations.computeIfAbsent(worker.worker, k -> new ArrayList<>()).addAll(tasks);
                    worker.assignedTasks.removeAll(tasks);
                    worker.unassignedTasks.addAll(tasks);
                    revoked += tasks.size();
                    
                    log.debug("Phase 3 - Revoked {} task from worker {} for connector {}", 
                            tasks.size(), worker.worker, connector);
                }
            }
        }
        
        return revocations;
    }
    
    // ===== Global Revocation =====
    
    private Map<String, Collection<ConnectorTaskId>> performGlobalRevocation(
            List<WorkerState> workers,
            BalanceTargets targets) {
        
        Map<String, Collection<ConnectorTaskId>> revocations = new HashMap<>();
        
        for (WorkerState worker : workers) {
            int totalTasks = worker.assignedTasks.size();
            if (totalTasks > targets.globalMaxLimit) {
                int toRevoke = totalTasks - targets.globalMaxLimit;
                
                // Select tasks while maintaining per-consumer balance
                List<ConnectorTaskId> candidates = selectTasksForGlobalRevocation(worker, targets, toRevoke);
                
                if (!candidates.isEmpty()) {
                    revocations.put(worker.worker, candidates);
                    worker.assignedTasks.removeAll(candidates);
                    worker.unassignedTasks.addAll(candidates);
                    
                    log.debug("Global Revocation - Revoked {} tasks from worker {}", 
                            candidates.size(), worker.worker);
                }
            }
        }
        
        return revocations;
    }
    
    private List<ConnectorTaskId> selectTasksForGlobalRevocation(
            WorkerState worker,
            BalanceTargets targets,
            int count) {
        
        List<ConnectorTaskId> candidates = new ArrayList<>();
        
        // Group tasks by connector
        Map<String, List<ConnectorTaskId>> tasksByConnector = new TreeMap<>();
        for (ConnectorTaskId task : worker.assignedTasks) {
            tasksByConnector.computeIfAbsent(task.connector(), k -> new ArrayList<>()).add(task);
        }
        
        // Sort connectors by how many tasks can be safely revoked
        List<Map.Entry<String, List<ConnectorTaskId>>> sortedEntries = tasksByConnector.entrySet().stream()
                .sorted((e1, e2) -> {
                    ConsumerTarget t1 = targets.perConsumerTargets.get(e1.getKey());
                    ConsumerTarget t2 = targets.perConsumerTargets.get(e2.getKey());
                    
                    int safeToRevoke1 = e1.getValue().size() - (t1 != null ? t1.cNmin : 0);
                    int safeToRevoke2 = e2.getValue().size() - (t2 != null ? t2.cNmin : 0);
                    
                    return Integer.compare(safeToRevoke2, safeToRevoke1);
                })
                .collect(Collectors.toList());
        
        // Revoke tasks while maintaining per-consumer balance
        for (Map.Entry<String, List<ConnectorTaskId>> entry : sortedEntries) {
            if (candidates.size() >= count) {
                break;
            }
            
            String connector = entry.getKey();
            List<ConnectorTaskId> tasks = entry.getValue();
            ConsumerTarget target = targets.perConsumerTargets.get(connector);
            
            if (target == null) {
                continue;
            }
            
            int currentCount = tasks.size();
            int safeToRevoke = currentCount - target.cNmin;
            
            if (safeToRevoke > 0) {
                int toRevoke = Math.min(safeToRevoke, count - candidates.size());
                Collections.sort(tasks);
                candidates.addAll(tasks.subList(0, toRevoke));
            }
        }
        
        return candidates;
    }
    
    // ===== Helper Methods =====
    
    private List<WorkerState> buildWorkerState(
            Map<String, ConnectorsAndTasks> memberAssignments,
            Set<ConnectorTaskId> configuredTasks,
            Set<String> configuredConnectors) {
        
        List<WorkerState> workers = new ArrayList<>();
        Set<ConnectorTaskId> allAssignedTasks = new TreeSet<>();
        
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            WorkerState worker = new WorkerState(entry.getKey());
            // Filter out tasks belonging to deleted connectors to avoid incorrect balance calculations
            List<ConnectorTaskId> validTasks = entry.getValue().tasks().stream()
                    .filter(task -> configuredConnectors.contains(task.connector()))
                    .collect(Collectors.toList());
            worker.assignedTasks.addAll(validTasks);
            worker.assignedConnectors.addAll(entry.getValue().connectors());
            allAssignedTasks.addAll(validTasks);
            workers.add(worker);
        }
        
        // Find unassigned tasks (distribute among workers for tracking)
        Set<ConnectorTaskId> unassignedTasks = new TreeSet<>(configuredTasks);
        unassignedTasks.removeAll(allAssignedTasks);
        
        if (!unassignedTasks.isEmpty() && !workers.isEmpty()) {
            // Add unassigned tasks to first worker for tracking
            workers.get(0).unassignedTasks.addAll(unassignedTasks);
        }
        
        return workers;
    }
    
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
        
        // Per-consumer targets
        Map<String, Integer> taskCountsByConnector = new TreeMap<>();
        for (ConnectorTaskId task : configuredTasks) {
            taskCountsByConnector.merge(task.connector(), 1, Integer::sum);
        }
        
        for (String connector : configuredConnectors) {
            int taskCount = taskCountsByConnector.getOrDefault(connector, 0);
            ConsumerTarget target = new ConsumerTarget();
            target.totalTasks = taskCount;
            target.cNmin = taskCount / numWorkers;
            target.cNmax = (taskCount + numWorkers - 1) / numWorkers;  // ceiling
            targets.perConsumerTargets.put(connector, target);
        }
        
        log.debug("Balance targets - globalMin: {}, globalMax: {}, globalMaxLimit: {}", 
                targets.globalMin, targets.globalMax, targets.globalMaxLimit);
        
        return targets;
    }
    
    private BalanceState analyzeBalance(List<WorkerState> workers, BalanceTargets targets) {
        BalanceState state = new BalanceState();
        state.perConsumerBalanced = true;
        state.globalBalanced = true;
        
        // Count unassigned tasks
        for (WorkerState worker : workers) {
            state.unassignedTaskCount += worker.unassignedTasks.size();
        }
        state.hasUnassignedTasks = state.unassignedTaskCount > 0;
        
        // Check per-consumer balance
        for (Map.Entry<String, ConsumerTarget> entry : targets.perConsumerTargets.entrySet()) {
            String connector = entry.getKey();
            ConsumerTarget target = entry.getValue();
            
            for (WorkerState worker : workers) {
                int count = worker.getTaskCountForConnector(connector);
                if (count > target.cNmax) {
                    state.hasWorkersAboveCNmax = true;
                    state.perConsumerBalanced = false;
                }
                if (count < target.cNmin) {
                    state.hasWorkersBelowCNmin = true;
                    state.perConsumerBalanced = false;
                }
            }
        }
        
        // Check global balance
        for (WorkerState worker : workers) {
            int totalTasks = worker.assignedTasks.size();
            if (totalTasks > targets.globalMaxLimit) {
                state.hasWorkersAboveGlobalMaxLimit = true;
                state.globalBalanced = false;
            }
            if (totalTasks < targets.globalMin) {
                state.globalBalanced = false;
            }
        }
        
        return state;
    }
    
    private List<ConnectorTaskId> selectTasksForRevocation(
            WorkerState worker,
            String connector,
            int count) {
        
        List<ConnectorTaskId> tasks = worker.assignedTasks.stream()
                .filter(t -> t.connector().equals(connector))
                .sorted()
                .collect(Collectors.toList());
        
        return new ArrayList<>(tasks.subList(0, Math.min(count, tasks.size())));
    }
    
    private List<ConnectorTaskId> selectTasksForAssignment(
            List<ConnectorTaskId> available,
            int count) {
        
        Collections.sort(available);
        return new ArrayList<>(available.subList(0, Math.min(count, available.size())));
    }
    
    private String findConsumerWithMostUnassigned(Map<String, List<ConnectorTaskId>> unassignedByConsumer) {
        return unassignedByConsumer.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .max(Comparator.comparingInt(e -> e.getValue().size()))
                .map(Map.Entry::getKey)
                .orElse(null);
    }
    
    /**
     * Find the consumer with the most unassigned tasks that the given worker can accept.
     * For small consumers (tasks ≤ workers), skip if worker already has 1 task from that consumer.
     */
    private String findConsumerWithMostUnassignedForWorker(
            Map<String, List<ConnectorTaskId>> unassignedByConsumer,
            WorkerState worker,
            BalanceTargets targets,
            int numWorkers) {
        
        return unassignedByConsumer.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .filter(e -> {
                    // Get the consumer name from any task in the list
                    String consumer = e.getKey();
                    ConsumerTarget target = targets.perConsumerTargets.get(consumer);
                    
                    if (target != null) {
                        int currentCount = worker.getTaskCountForConnector(consumer);
                        
                        // For small consumers (tasks ≤ workers), skip if worker already has 1 task
                        if (target.totalTasks <= numWorkers) {
                            if (currentCount >= 1) {
                                return false; // Skip this consumer for this worker
                            }
                        }
                        
                        // For all consumers, skip if worker is already at cNmax
                        if (currentCount >= target.cNmax) {
                            return false; // Skip this consumer for this worker
                        }
                    }
                    return true;
                })
                .max(Comparator.comparingInt(e -> e.getValue().size()))
                .map(Map.Entry::getKey)
                .orElse(null);
    }
    
    // ===== Inner Classes =====
    
    static class WorkerState {
        final String worker;
        final Set<ConnectorTaskId> assignedTasks = new LinkedHashSet<>();
        final Set<ConnectorTaskId> unassignedTasks = new LinkedHashSet<>();
        final Set<String> assignedConnectors = new LinkedHashSet<>();
        
        WorkerState(String worker) {
            this.worker = worker;
        }
        
        int getTaskCountForConnector(String connector) {
            return (int) assignedTasks.stream()
                    .filter(t -> t.connector().equals(connector))
                    .count();
        }
    }
    
    static class BalanceTargets {
        int totalTasks;
        int globalMin;
        int globalMax;
        int globalMaxLimit;
        final Map<String, ConsumerTarget> perConsumerTargets = new TreeMap<>();
    }
    
    static class ConsumerTarget {
        int totalTasks;
        int cNmin;
        int cNmax;
    }
    
    static class BalanceState {
        boolean perConsumerBalanced = true;
        boolean globalBalanced = true;
        boolean hasUnassignedTasks = false;
        boolean hasWorkersAboveCNmax = false;
        boolean hasWorkersBelowCNmin = false;
        boolean hasWorkersAboveGlobalMaxLimit = false;
        int unassignedTaskCount = 0;
    }

}
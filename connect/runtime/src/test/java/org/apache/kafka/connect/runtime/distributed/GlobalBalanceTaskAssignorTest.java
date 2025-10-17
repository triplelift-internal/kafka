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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAssignor.ClusterAssignment;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.util.ConnectUtils.transformValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for GlobalBalanceTaskAssignor which verifies multi-round cooperative rebalancing
 * to achieve per-consumer and global balance across the cluster.
 * 
 * Key differences from IncrementalCooperativeAssignorTest:
 * 1. Tests per-consumer balance (tasks grouped by consumer prefix)
 * 2. Verifies multi-round convergence to balanced state
 * 3. Tests interaction between parent's cooperative rebalancing and per-consumer balancing
 * 
 * Test Configuration:
 * - Initial cluster: 5 workers
 * - Scale up scenarios: Add 25 workers (total 30)
 * - Scale down scenarios: Remove 2 workers (down to 3)
 * - 60 Consumer groups with varying task counts:
 *   - C1: 1024 tasks
 *   - C2-C3: 864 tasks each (2 consumers)
 *   - C4-C5: 244 tasks each (2 consumers)
 *   - C6: 210 tasks
 *   - C7: 192 tasks
 *   - C8: 144 tasks
 *   - C9: 99 tasks
 *   - C10: 89 tasks
 *   - C11: 49 tasks
 *   - C12: 29 tasks
 *   - C13: 7 tasks
 *   - C14-C15: 6 tasks each (2 consumers)
 *   - C16-C30: 3 tasks each (15 consumers)
 *   - C31-C43: 2 tasks each (13 consumers)
 *   - C44-C60: 1 task each (17 consumers)
 *   Total: 4192 tasks across 60 consumers
 */
public class GlobalBalanceTaskAssignorTest {
    private static final Logger log = LoggerFactory.getLogger(GlobalBalanceTaskAssignorTest.class);
    private static final long CONFIG_OFFSET = 618;

    private LogContext logContext;
    private MockTime time;
    private int rebalanceDelay;
    private GlobalBalanceTaskAssignor assignor;
    private int generationId;
    private ClusterAssignment returnedAssignments;
    private Map<String, ConnectorsAndTasks> memberAssignments;
    private Map<String, Integer> connectors;

    @Before
    public void setup() {
        generationId = 1000;
        logContext = new LogContext();
        time = new MockTime();
        rebalanceDelay = DistributedConfig.SCHEDULED_REBALANCE_MAX_DELAY_MS_DEFAULT;
        connectors = new HashMap<>();
        memberAssignments = new HashMap<>();
        // Start with 5 workers for large-scale testing
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        initAssignor();
    }

    private void initAssignor() {
        assignor = new GlobalBalanceTaskAssignor(logContext, time, rebalanceDelay);
        assignor.previousGenerationId = generationId;
    }

    /**
     * Add fairly large-scale consumer distribution:
     * - C1: 1024 tasks
     * - C2-C3: 864 tasks each (2 consumers)
     * - C4-C5: 244 tasks each (2 consumers)
     * - C6: 210 tasks
     * - C7: 192 tasks
     * - C8: 144 tasks
     * - C9: 99 tasks
     * - C10: 89 tasks
     * - C11: 49 tasks
     * - C12: 29 tasks
     * - C13: 7 tasks
     * - C14-C15: 6 tasks each (2 consumers)
     * - C16-C30: 3 tasks each (15 consumers)
     * - C31-C43: 2 tasks each (13 consumers)
     * - C44-C60: 1 task each (17 consumers)
     * Total: 4192 tasks across 60 consumers
     */
    private void addStandardConsumerDistribution() {
        // Large consumers
        addNewConnector("C1-connector1", 1024);
        addNewConnector("C2-connector1", 864);
        addNewConnector("C3-connector1", 864);
        addNewConnector("C4-connector1", 244);
        addNewConnector("C5-connector1", 244);
        addNewConnector("C6-connector1", 210);
        addNewConnector("C7-connector1", 192);
        addNewConnector("C8-connector1", 144);
        addNewConnector("C9-connector1", 99);
        addNewConnector("C10-connector1", 89);
        addNewConnector("C11-connector1", 49);
        addNewConnector("C12-connector1", 29);
        addNewConnector("C13-connector1", 7);
        addNewConnector("C14-connector1", 6);
        addNewConnector("C15-connector1", 6);
        
        // C16-C30: 3 tasks each (15 consumers)
        for (int i = 16; i <= 30; i++) {
            addNewConnector("C" + i + "-connector1", 3);
        }
        
        // C31-C43: 2 tasks each (13 consumers)
        for (int i = 31; i <= 43; i++) {
            addNewConnector("C" + i + "-connector1", 2);
        }
        
        // C44-C60: 1 task each (17 consumers)
        for (int i = 44; i <= 60; i++) {
            addNewConnector("C" + i + "-connector1", 1);
        }
    }

    // ==================== Multi-Round Per-Consumer Balance Tests ====================

    /**
     * Test that per-consumer balance is achieved through multiple rebalancing rounds.
     * 
     * Scenario:
     * - 5 initial workers
     * - 60 consumers with varying task counts (total 4192 tasks)
     * - Initial imbalanced state
     * - Verify multi-round convergence to per-consumer balanced state
     * - Verify global balance is also maintained
     */
    @Test
    public void testPerConsumerBalanceThroughMultipleRounds() {
        // Setup: Large-scale consumer distribution (4192 tasks across 60 consumers)
        addStandardConsumerDistribution();
        
        // Round 1: Initial assignment with 5 workers (4192 tasks / 5 = ~838 tasks per worker)
        performStandardRebalance();
        assertDelay(0);
        assertAllWorkersAssigned();
        
        // Converge to balanced state through multiple rounds
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Converged to balanced state in {} rounds", i + 1);
                break;
            }
        }
        
        assertBalancedAndCompleteAllocation();
        assertGlobalBalance();
        
        // Verify all consumers are balanced
        assertAllConsumersBalanced();
        
        log.info("Final distribution - Per-consumer: {}, Global: {}", 
                formatPerConsumerDistribution(), formatGlobalDistribution());
    }

    /**
     * Test per-consumer balancing with imbalanced initial state.
     * 
     * Scenario:
     * - 5 workers with pre-existing imbalanced assignments
     * - 60 consumers with large-scale task distribution (4192 tasks)
     * - Verify all consumers are rebalanced
     * - Verify global balance is maintained
     */
    @Test
    public void testPerConsumerBalanceWithImbalancedInitialState() {
        // Setup: Large-scale consumer distribution
        addStandardConsumerDistribution();
        
        // First, let the system achieve initial balance
        performStandardRebalance();
        assertAllWorkersAssigned();
        
        // Converge to balanced state
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Initial balance achieved in {} rounds", i + 1);
                break;
            }
        }
        
        // Verify final state - both per-consumer AND global balance
        assertTrue("Per-consumer balance should be achieved", isPerConsumerBalanced());
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
        
        // Verify all consumers are balanced
        assertAllConsumersBalanced();
        
        log.info("Final state - Per-consumer: {}, Global: {}", 
                formatPerConsumerDistribution(), formatGlobalDistribution());
    }

    /**
     * Test that GlobalBalanceTaskAssignor respects parent's delayed rebalance during scale-down.
     * 
     * Scenario:
     * - Start with 5 workers and large-scale task distribution (4192 tasks)
     * - Remove 2 workers (scale down) which triggers delayed rebalance
     * - Verify per-consumer balancing waits for delay to expire
     * - Verify per-consumer balancing proceeds after delay
     */
    @Test
    public void testPerConsumerBalanceRespectsDelayedRebalance() {
        // Setup initial balanced state with 5 workers and large-scale distribution
        addStandardConsumerDistribution();
        
        performStandardRebalance();
        assertAllWorkersAssigned();
        
        // Converge to balanced state
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isBalancedAndComplete()) break;
        }
        assertBalancedAndCompleteAllocation();
        
        // Scale down: Remove 2 workers (leaving 3 workers)
        removeWorkers("worker4", "worker5");
        performStandardRebalance();
        assertTrue("Delay should be set", assignor.delay > 0);
        assertAllWorkersAssigned();
        
        // Per-consumer balancing should NOT happen during delay
        performStandardRebalance();
        assertTrue("Delay should still be active", assignor.delay > 0);
        
        // Fast-forward past delay
        time.sleep(assignor.delay);
        
        // Now per-consumer balancing should proceed
        performStandardRebalance();
        assertDelay(0);
        
        // Converge to final balanced state
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isBalancedAndComplete()) break;
        }
        assertBalancedAndCompleteAllocation();
        
        // Verify all consumers are still balanced
        assertAllConsumersBalanced();
    }

    /**
     * Test per-consumer balance with scale-up scenario (adding 25 workers).
     * 
     * Scenario:
     * - Start with 5 workers
     * - 60 consumers with large-scale task distribution (4192 tasks)
     * - Add 25 workers (scale up to 30 total)
     * - Verify each consumer achieves per-consumer balance independently
     * - Verify global balance is maintained (4192 tasks / 30 workers ≈ 140 tasks per worker)
     */
    @Test
    public void testMultipleConsumersWithDifferentTaskCounts() {
        // Setup: Large-scale consumer distribution (4192 tasks across 60 consumers)
        addStandardConsumerDistribution();
        
        // Initial assignment with 5 workers
        performStandardRebalance();
        assertAllWorkersAssigned();
        
        // Converge to initial balanced state
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Initial balance with 5 workers achieved in {} rounds", i + 1);
                break;
            }
        }
        
        // Scale up: Add 25 workers for total of 30 workers
        String[] newWorkers = new String[25];
        for (int i = 0; i < 25; i++) {
            newWorkers[i] = "worker" + (i + 6);  // worker6 through worker30
        }
        addNewEmptyWorkers(newWorkers);
        performStandardRebalance();
        
        // Converge to balanced state with 30 workers (4192 tasks / 30 = ~140 per worker)
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Converged to balanced state with 30 workers in {} rounds", i + 1);
                break;
            }
        }
        
        // Verify all consumers have per-consumer balance
        assertAllConsumersBalanced();
        
        // Verify global balance (each worker should have ~140 tasks: 4192/30 ≈ 140)
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
        
        log.info("Final distribution with 30 workers - Global: {}", 
                formatGlobalDistribution());
    }

    /**
     * Test worker joining during per-consumer rebalancing (scale-up scenario).
     * 
     * Scenario:
     * - Initial 5 workers with large-scale task distribution
     * - Add 25 workers during rebalancing (scale up to 30 total)
     * - Verify smooth convergence with new workers
     */
    @Test
    public void testWorkerJoiningDuringPerConsumerRebalance() {
        // Setup: Large-scale consumer distribution
        addStandardConsumerDistribution();
        
        // Initial assignment with 5 workers
        performStandardRebalance();
        assertAllWorkersAssigned();
        
        // Converge to initial balanced state
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Initial balance achieved in {} rounds", i + 1);
                break;
            }
        }
        
        assertGlobalBalance();
        
        // Scale up: Add 25 workers during rebalance
        String[] newWorkers = new String[25];
        for (int i = 0; i < 25; i++) {
            newWorkers[i] = "worker" + (i + 6);  // worker6 through worker30
        }
        addNewEmptyWorkers(newWorkers);
        performStandardRebalance();
        
        // Converge again with 30 workers (4192 tasks / 30 workers = ~140 tasks per worker)
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                log.info("Rebalanced after scale-up in {} rounds", i + 1);
                break;
            }
        }
        
        // Verify all consumers are balanced
        assertAllConsumersBalanced();
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
        
        log.info("Final distribution with 30 workers - Global: {}", 
                formatGlobalDistribution());
    }

    /**
     * Test connector deletion during per-consumer rebalancing.
     * 
     * Scenario:
     * - Ongoing per-consumer rebalancing with large-scale distribution
     * - Connector is deleted
     * - Verify graceful handling and continued balance of remaining connectors
     */
    @Test
    public void testConnectorDeletionDuringPerConsumerRebalance() {
        // Setup: Large-scale consumer distribution
        addStandardConsumerDistribution();
        
        // Initial assignment with 5 workers
        performStandardRebalance();
        performStandardRebalance();
        
        // Delete one of the larger connectors (C1 with 1024 tasks)
        removeConnector("C1-connector1");
        performStandardRebalance();
        
        // Converge to balanced state (now 3168 tasks across 5 workers)
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                break;
            }
        }
        
        // Verify all remaining consumers are balanced
        assertAllConsumersBalanced();
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
    }

    /**
     * Test that per-consumer balance takes priority over global balance.
     * 
     * Scenario:
     * - Large-scale consumer distribution with varying task counts
     * - Verify per-consumer balance is achieved for each consumer
     * - Verify global balance is also achieved
     */
    @Test
    public void testPerConsumerBalancePriority() {
        // Setup: Large-scale consumer distribution
        addStandardConsumerDistribution();
        
        // Initial assignment and convergence with 5 workers
        performStandardRebalance();
        
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced()) {
                log.info("Per-consumer balance achieved in round {}", i + 1);
                break;
            }
        }
        
        // Verify all consumers have per-consumer balance
        assertAllConsumersBalanced();
        
        // Global balance should also be maintained (within tolerance)
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
        
        log.info("Final distribution - Global: {}", formatGlobalDistribution());
    }

    /**
     * Test global balance with large-scale uneven consumer task distribution.
     * 
     * Scenario:
     * - 60 consumers with highly varying task counts (1 to 1024 tasks)
     * - 5 workers
     * - Total: 4192 tasks → ~838 per worker for global balance
     * - Verify both per-consumer and global balance are achieved
     */
    @Test
    public void testGlobalBalanceWithUnevenConsumerDistribution() {
        // Setup: Large-scale consumer distribution with highly uneven task counts
        addStandardConsumerDistribution();
        
        // Converge through multiple rounds with 5 workers
        performStandardRebalance();
        
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced()) {
                log.info("Converged in {} rounds", i + 1);
                break;
            }
        }
        
        // Verify all consumers have per-consumer balance
        assertAllConsumersBalanced();
        
        // Verify global balance: each worker should have ~838 tasks (4192/5)
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
        
        log.info("Final distribution - Global: {}", formatGlobalDistribution());
    }

    /**
     * Test zero-delay rebalancing with per-consumer balance and large-scale distribution.
     * 
     * Scenario:
     * - scheduled.rebalance.max.delay.ms = 0
     * - Large-scale consumer distribution (4192 tasks)
     * - Verify per-consumer balancing happens immediately without delays
     */
    @Test
    public void testPerConsumerBalanceWithZeroDelay() {
        // Reconfigure with zero delay
        rebalanceDelay = 0;
        initAssignor();
        
        // Setup: Large-scale consumer distribution
        addStandardConsumerDistribution();
        
        // Should converge quickly without delays with 5 workers
        performStandardRebalance();
        assertDelay(0);
        
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertDelay(0);  // No delays should be set
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isPerConsumerBalanced() && isGlobalBalanced() && isBalancedAndComplete()) {
                break;
            }
        }
        
        // Verify all consumers are balanced
        assertAllConsumersBalanced();
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
    }

    /**
     * Test single consumer with large task count (no per-consumer balancing needed).
     * Verify it falls back to parent's global balancing.
     */
    @Test
    public void testSingleConsumerFallsBackToGlobalBalance() {
        // Single consumer with 1024 tasks across 5 workers
        addNewConnector("C1-connector1", 1024);
        
        performStandardRebalance();
        
        // Converge (1024 tasks / 5 workers ≈ 205 per worker)
        int maxRounds = 6;
        for (int i = 0; i < maxRounds; i++) {
            performStandardRebalance();
            assertNoDuplicateAllocations();  // Verify no duplicates after each round
            if (isGlobalBalanced() && isBalancedAndComplete()) break;
        }
        
        // With single consumer, per-consumer balance = global balance
        assertPerConsumerBalance("C1");
        assertGlobalBalance();
        assertBalancedAndCompleteAllocation();
    }

    /**
     * Test empty cluster (no connectors/tasks) with 5 workers.
     */
    @Test
    public void testEmptyClusterHandling() {
        // 5 workers already available from setup, no connectors
        
        performStandardRebalance();
        assertAllWorkersAssigned();
        assertEmptyAssignment();
        assertBalancedAndCompleteAllocation();
    }

    // ==================== Helper Methods ====================

    private void performStandardRebalance() {
        performRebalance(false);
    }

    private void performRebalance(boolean assignmentFailure) {
        generationId++;
        int lastCompletedGenerationId = generationId - 1;
        
        try {
            Map<String, ConnectorsAndTasks> memberAssignmentsCopy = memberAssignments.entrySet().stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new ConnectorsAndTasks.Builder()
                                    .with(e.getValue().connectors(), e.getValue().tasks())
                                    .build()
                    ));
            
            returnedAssignments = assignor.performTaskAssignment(
                    configState(), 
                    lastCompletedGenerationId, 
                    generationId, 
                    memberAssignmentsCopy
            );
        } catch (RuntimeException e) {
            if (!assignmentFailure) {
                throw e;
            }
        }
        
        if (!assignmentFailure) {
            applyAssignments();
        }
    }

    private void applyAssignments() {
        returnedAssignments.allWorkers().forEach(worker -> {
            ConnectorsAndTasks workerAssignment = memberAssignments.computeIfAbsent(
                    worker, 
                    ignored -> new ConnectorsAndTasks.Builder().build()
            );

            workerAssignment.connectors().removeAll(returnedAssignments.newlyRevokedConnectors(worker));
            workerAssignment.connectors().addAll(returnedAssignments.newlyAssignedConnectors(worker));
            workerAssignment.tasks().removeAll(returnedAssignments.newlyRevokedTasks(worker));
            workerAssignment.tasks().addAll(returnedAssignments.newlyAssignedTasks(worker));
        });
    }

    private void addNewEmptyWorkers(String... workers) {
        for (String worker : workers) {
            addNewWorker(worker, Collections.emptyList(), Collections.emptyList());
        }
    }

    private void addNewWorker(String worker, List<String> connectors, List<ConnectorTaskId> tasks) {
        ConnectorsAndTasks assignment = new ConnectorsAndTasks.Builder()
                .with(connectors, tasks)
                .build();
        memberAssignments.put(worker, assignment);
    }

    private void removeWorkers(String... workers) {
        for (String worker : workers) {
            assertNotNull("Worker " + worker + " does not exist", memberAssignments.remove(worker));
        }
    }

    private void addNewConnector(String connector, int taskCount) {
        connectors.put(connector, taskCount);
    }

    private void removeConnector(String connector) {
        assertNotNull("Connector " + connector + " does not exist", connectors.remove(connector));
    }

    private ClusterConfigState configState() {
        Map<String, Integer> taskCounts = new HashMap<>(connectors);
        Map<String, Map<String, String>> connectorConfigs = transformValues(taskCounts, c -> Collections.emptyMap());
        Map<String, TargetState> targetStates = transformValues(taskCounts, c -> TargetState.STARTED);
        Map<ConnectorTaskId, Map<String, String>> taskConfigs = taskCounts.entrySet().stream()
                .flatMap(e -> IntStream.range(0, e.getValue())
                        .mapToObj(i -> new ConnectorTaskId(e.getKey(), i)))
                .collect(Collectors.toMap(
                        Function.identity(),
                        connectorTaskId -> Collections.emptyMap()
                ));
        
        return new ClusterConfigState(
                CONFIG_OFFSET,
                null,
                taskCounts,
                connectorConfigs,
                targetStates,
                taskConfigs,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
        );
    }

    private boolean isPerConsumerBalanced() {
        Map<String, Map<String, List<ConnectorTaskId>>> distribution = analyzePerConsumerDistribution();
        
        // Get all workers in the cluster
        Set<String> allWorkers = memberAssignments.keySet();
        
        for (Map.Entry<String, Map<String, List<ConnectorTaskId>>> entry : distribution.entrySet()) {
            String consumer = entry.getKey();
            Map<String, List<ConnectorTaskId>> workerTasks = entry.getValue();
            
            if (workerTasks.isEmpty()) continue;
            
            // Calculate min/max across ALL workers (including those with 0 tasks)
            int min = Integer.MAX_VALUE;
            int max = 0;
            
            for (String worker : allWorkers) {
                int taskCount = workerTasks.getOrDefault(worker, Collections.emptyList()).size();
                min = Math.min(min, taskCount);
                max = Math.max(max, taskCount);
            }
            
            // Reset min if all workers have 0 tasks
            if (min == Integer.MAX_VALUE) {
                min = 0;
            }
            
            if (max - min > 1) {
                log.debug("Consumer {} not balanced: min={}, max={}, diff={}", consumer, min, max, max - min);
                return false;
            }
        }
        
        return true;
    }

    private boolean isGlobalBalanced() {
        try {
            assertGlobalBalance();
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }

    private boolean isBalancedAndComplete() {
        try {
            assertNoDuplicateAllocations();  // Check for duplicates first
            assertBalancedAndCompleteAllocation();
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }

    private Map<String, Map<String, List<ConnectorTaskId>>> analyzePerConsumerDistribution() {
        Map<String, Map<String, List<ConnectorTaskId>>> result = new HashMap<>();
        
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            for (ConnectorTaskId task : entry.getValue().tasks()) {
                String consumer = extractConsumer(task.connector());
                result.computeIfAbsent(consumer, k -> new HashMap<>())
                      .computeIfAbsent(worker, k -> new ArrayList<>())
                      .add(task);
            }
        }
        
        return result;
    }

    private String extractConsumer(String connector) {
        int dashIndex = connector.indexOf('-');
        if (dashIndex > 0) {
            return connector.substring(0, dashIndex);
        }
        return "connect-" + connector;
    }

    private void assertPerConsumerBalance(String consumer) {
        Map<String, Map<String, List<ConnectorTaskId>>> distribution = analyzePerConsumerDistribution();
        Map<String, List<ConnectorTaskId>> workerTasks = distribution.get(consumer);
        
        if (workerTasks == null || workerTasks.isEmpty()) {
            return; // No tasks for this consumer
        }
        
        // Get all workers in the cluster
        Set<String> allWorkers = memberAssignments.keySet();
        
        // Calculate min/max across ALL workers (including those with 0 tasks)
        int min = Integer.MAX_VALUE;
        int max = 0;
        
        for (String worker : allWorkers) {
            int taskCount = workerTasks.getOrDefault(worker, Collections.emptyList()).size();
            min = Math.min(min, taskCount);
            max = Math.max(max, taskCount);
        }
        
        // Reset min if all workers have 0 tasks
        if (min == Integer.MAX_VALUE) {
            min = 0;
        }
        
        assertTrue(
                String.format("Consumer %s not balanced: min=%d, max=%d, diff=%d. Distribution: %s", 
                        consumer, min, max, max - min, formatDistribution(workerTasks)),
                max - min <= 1
        );
    }

    private void assertAllConsumersBalanced() {
        // Get all unique consumers from current member assignments
        Map<String, Map<String, List<ConnectorTaskId>>> distribution = analyzePerConsumerDistribution();
        
        // Verify balance for each consumer
        for (String consumer : distribution.keySet()) {
            assertPerConsumerBalance(consumer);
        }
    }

    private void assertGlobalBalance() {
        List<Integer> taskCounts = memberAssignments.values().stream()
                .map(a -> a.tasks().size())
                .sorted()
                .collect(Collectors.toList());
        
        if (taskCounts.isEmpty()) {
            return; // No tasks to balance
        }
        
        int minTasks = taskCounts.get(0);
        int maxTasks = taskCounts.get(taskCounts.size() - 1);
        
        assertTrue(
                String.format("Global balance not achieved: min=%d, max=%d, diff=%d. Distribution: %s",
                        minTasks, maxTasks, maxTasks - minTasks, formatGlobalDistribution()),
                maxTasks - minTasks <= 1
        );
    }

    private String formatDistribution(Map<String, List<ConnectorTaskId>> workerTasks) {
        return workerTasks.entrySet().stream()
                .map(e -> e.getKey() + ":" + e.getValue().size())
                .collect(Collectors.joining(", "));
    }

    private String formatPerConsumerDistribution() {
        Map<String, Map<String, List<ConnectorTaskId>>> distribution = analyzePerConsumerDistribution();
        return distribution.entrySet().stream()
                .map(consumerEntry -> {
                    String consumer = consumerEntry.getKey();
                    String workerDist = consumerEntry.getValue().entrySet().stream()
                            .map(e -> e.getKey() + ":" + e.getValue().size())
                            .collect(Collectors.joining(","));
                    return consumer + "={" + workerDist + "}";
                })
                .collect(Collectors.joining("; "));
    }

    private String formatGlobalDistribution() {
        return memberAssignments.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + ":" + e.getValue().tasks().size())
                .collect(Collectors.joining(", "));
    }

    private void assertEmptyAssignment() {
        assertTrue("Should have no newly assigned connectors",
                returnedAssignments.newlyAssignedConnectors().values().stream().allMatch(Collection::isEmpty));
        assertTrue("Should have no newly assigned tasks",
                returnedAssignments.newlyAssignedTasks().values().stream().allMatch(Collection::isEmpty));
        assertTrue("Should have no newly revoked connectors",
                returnedAssignments.newlyRevokedConnectors().values().stream().allMatch(Collection::isEmpty));
        assertTrue("Should have no newly revoked tasks",
                returnedAssignments.newlyRevokedTasks().values().stream().allMatch(Collection::isEmpty));
    }

    private void assertAllWorkersAssigned() {
        // Verify all workers in memberAssignments are present in returned assignments
        Set<String> expectedWorkers = memberAssignments.keySet();
        Set<String> actualWorkers = returnedAssignments.allWorkers();
        
        assertEquals("Worker set mismatch between memberAssignments and returnedAssignments",
                expectedWorkers,
                actualWorkers);
    }

    private void assertDelay(int expectedDelay) {
        assertEquals("Wrong rebalance delay", expectedDelay, assignor.delay);
    }

    private void assertBalancedAndCompleteAllocation() {
        assertNoDuplicateAllocations();
        assertBalancedAllocation();
        assertCompleteAllocation();
    }

    /**
     * Verify that no task is assigned to multiple workers (duplicate allocation).
     * This is a critical invariant that must hold after every rebalancing round.
     */
    private void assertNoDuplicateAllocations() {
        Map<ConnectorTaskId, List<String>> taskToWorkers = new HashMap<>();
        
        // Build map of task -> list of workers that have it
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            for (ConnectorTaskId task : entry.getValue().tasks()) {
                taskToWorkers.computeIfAbsent(task, k -> new ArrayList<>()).add(worker);
            }
        }
        
        // Find any tasks assigned to multiple workers
        List<String> duplicates = taskToWorkers.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .map(e -> e.getKey() + " assigned to " + e.getValue())
            .collect(Collectors.toList());
        
        assertTrue(
            "Found duplicate task assignments: " + String.join(", ", duplicates),
            duplicates.isEmpty()
        );
        
        // Also verify connectors have no duplicates
        Map<String, List<String>> connectorToWorkers = new HashMap<>();
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            for (String connector : entry.getValue().connectors()) {
                connectorToWorkers.computeIfAbsent(connector, k -> new ArrayList<>()).add(worker);
            }
        }
        
        List<String> connectorDuplicates = connectorToWorkers.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .map(e -> e.getKey() + " assigned to " + e.getValue())
            .collect(Collectors.toList());
        
        assertTrue(
            "Found duplicate connector assignments: " + String.join(", ", connectorDuplicates),
            connectorDuplicates.isEmpty()
        );
    }

    private void assertBalancedAllocation() {
        List<Integer> connectorCounts = memberAssignments.values().stream()
                .map(a -> a.connectors().size())
                .sorted()
                .collect(Collectors.toList());
        List<Integer> taskCounts = memberAssignments.values().stream()
                .map(a -> a.tasks().size())
                .sorted()
                .collect(Collectors.toList());

        if (!connectorCounts.isEmpty()) {
            int minConnectors = connectorCounts.get(0);
            int maxConnectors = connectorCounts.get(connectorCounts.size() - 1);
            assertTrue("Connector assignments are imbalanced: " + connectorCounts,
                    maxConnectors - minConnectors <= 1);
        }

        if (!taskCounts.isEmpty()) {
            int minTasks = taskCounts.get(0);
            int maxTasks = taskCounts.get(taskCounts.size() - 1);
            assertTrue("Task assignments are imbalanced: " + taskCounts,
                    maxTasks - minTasks <= 1);
        }
    }

    private void assertCompleteAllocation() {
        Set<String> allAssignedConnectors = memberAssignments.values().stream()
                .flatMap(a -> a.connectors().stream())
                .collect(Collectors.toSet());
        assertEquals("Wrong set of assigned connectors",
                connectors.keySet(),
                allAssignedConnectors);

        Map<String, Set<ConnectorTaskId>> allAssignedTasks = memberAssignments.values().stream()
                .flatMap(a -> a.tasks().stream())
                .collect(Collectors.groupingBy(
                        ConnectorTaskId::connector,
                        Collectors.toSet()
                ));

        connectors.forEach((connector, taskCount) -> {
            Set<ConnectorTaskId> expectedTasks = IntStream.range(0, taskCount)
                    .mapToObj(i -> new ConnectorTaskId(connector, i))
                    .collect(Collectors.toSet());
            Set<ConnectorTaskId> actualTasks = allAssignedTasks.getOrDefault(connector, Collections.emptySet());
            assertEquals("Wrong tasks for connector " + connector,
                    expectedTasks,
                    actualTasks);
        });
    }
}
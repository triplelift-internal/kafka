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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.util.ConnectUtils.transformValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Comprehensive tests for BalancedCooperativeAssignor covering all scenarios.
 * 
 * Tests verify the two-round protocol:
 * - Round 1 (Revocations): Full revocation when triggered by empty workers or severe imbalance
 * - Round 2 (Assignments): Incremental assignment with Phase A (consumerN_task_count_min) and Phase B (remaining)
 * 
 * Scenarios tested (from algorithm specification):
 * 1. Scale-up (empty workers) → Round 1 + Round 2
 * 2. Scale-down (workers left) → Round 2 only
 * 3. Task count increase → Round 2 only
 * 4. New connector added → Round 1 (if creates empty workers) + Round 2
 * 5. Connector removed → Round 2 only
 * 6. Task count decrease → Round 2 only
 */
public class BalancedCooperativeAssignorTest {
    private static final Logger log = LoggerFactory.getLogger(BalancedCooperativeAssignorTest.class);
    private static final long CONFIG_OFFSET = 10;

    private LogContext logContext;
    private MockTime time;
    private int rebalanceDelay;
    private BalancedCooperativeAssignor assignor;
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
        initAssignor();
    }

    private void initAssignor() {
        assignor = new BalancedCooperativeAssignor(logContext, time, rebalanceDelay);
        assignor.previousGenerationId = generationId;
    }

    // ==================== SCENARIO 1: Scale-Up (Empty Workers) ====================
    
    /**
     * Scenario 1: Scale-Up from 0 to 3 workers
     * Expected Flow:
     * - Round 1: Full revocation (all tasks unassigned)
     * - Round 2: Complete assignment with perfect balance
     * 
     * This tests the classic scale-up scenario where new workers join an empty cluster.
     */
    @Test
    public void testScenario1_ScaleUpFrom0To3Workers() {
        log.info("=== SCENARIO 1: Scale-Up (0→3 workers) ===");
        
        // Start with 3 empty workers
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        
        // Add 1 connector with 10 tasks
        addNewConnector("connector-1", 10);
        
        // Round 1 would be triggered (empty workers), but there's nothing to revoke
        // So we skip directly to Round 2 (assignments) in a single rebalance
        performStandardRebalance();
        
        log.info("After rebalance (Round 2 - assignments):");
        logAllAssignments();
        
        // Verify: All 10 tasks assigned in single rebalance, no revocations
        assertRound2Behavior(10, 0);  // 10 assignments, 0 revocations
        
        // Verify final balance: 3-4 tasks per worker
        assertBalancedDistribution(3, 4);
        assertAllTasksAssigned();
        assertNoDuplicateConnectorAssignments();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 1 passed: Scale-up handled correctly");
    }
    
    /**
     * Scenario 1b: Scale-Up from 2 to 5 workers (with existing load)
     * Expected Flow:
     * - Round 1: Full revocation from all workers
     * - Round 2: Redistribute across all 5 workers
     */
    @Test
    public void testScenario1b_ScaleUpFrom2To5WorkersWithExistingLoad() {
        log.info("=== SCENARIO 1b: Scale-Up (2→5 workers with existing load) ===");
        
        // Start with 2 workers, balanced assignment
        addNewEmptyWorkers("worker1", "worker2");
        addNewConnector("connector-1", 10);
        performStandardRebalance();
        
        log.info("Initial state (2 workers, 10 tasks):");
        logAllAssignments();
        assertEquals(5, memberAssignments.get("worker1").tasks().size());
        assertEquals(5, memberAssignments.get("worker2").tasks().size());
        
        // Add 3 new empty workers
        addNewEmptyWorkers("worker3", "worker4", "worker5");
        
        log.info("After adding 3 empty workers:");
        logAllAssignments();
        
        // Round 1: Should revoke all tasks from existing workers
        performStandardRebalance();
        
        log.info("After Round 1 (Revocation):");
        logAllAssignments();
        
        // Verify Round 1: All 10 tasks revoked, no assignments yet
        assertRound1Behavior(10, 0);
        
        // All workers should now have 0 tasks after Round 1
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            assertEquals("After Round 1, all workers should have 0 tasks", 
                    0, assignment.tasks().size());
        }
        
        // Round 2: Assign all tasks across 5 workers
        performStandardRebalance();
        
        log.info("After Round 2 (Assignment):");
        logAllAssignments();
        
        // Verify Round 2: All 10 tasks assigned, no revocations
        assertRound2Behavior(10, 0);
        
        // Verify final balance: 2 tasks per worker (10 tasks / 5 workers)
        assertBalancedDistribution(2, 2);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 1b passed: Scale-up with existing load handled correctly");
    }

    // ==================== SCENARIO 2: Scale-Down (Workers Left) ====================
    
    /**
     * Scenario 2: Scale-Down from 5 to 3 workers
     * Expected Flow:
     * - Round 1: SKIPPED (no empty workers, no severe imbalance)
     * - Round 2: Incremental assignment of lost tasks only
     * 
     * Key: Existing tasks on remaining workers are preserved!
     */
    @Test
    public void testScenario2_ScaleDownFrom5To3Workers() {
        log.info("=== SCENARIO 2: Scale-Down (5→3 workers) ===");
        
        // Start with 5 workers, balanced
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        addNewConnector("connector-1", 10);
        performStandardRebalance();
        performStandardRebalance();  // Ensure fully balanced
        
        log.info("Initial state (5 workers, 10 tasks):");
        logAllAssignments();
        assertBalancedDistribution(2, 2);
        
        // Track which tasks worker4 and worker5 had
        Set<ConnectorTaskId> lostTasks = new HashSet<>();
        lostTasks.addAll(memberAssignments.get("worker4").tasks());
        lostTasks.addAll(memberAssignments.get("worker5").tasks());
        int lostTaskCount = lostTasks.size();
        
        log.info("Tasks that will be lost: {}", lostTasks);
        
        // Remove 2 workers
        removeWorkers("worker4", "worker5");
        
        log.info("After removing worker4 and worker5:");
        logAllAssignments();
        
        // Round 2: Should only assign lost tasks (no revocations)
        performStandardRebalance();
        
        log.info("After Round 2 (Incremental Assignment):");
        logAllAssignments();
        
        // Verify Round 2 only: Lost tasks assigned, no revocations
        assertRound2Behavior(lostTaskCount, 0);
        
        // Verify no Round 1 happened (no revocations from existing workers)
        // Each remaining worker should have gained some tasks, not lost any
        
        // Verify final balance: 3-4 tasks per worker (10 tasks / 3 workers)
        assertBalancedDistribution(3, 4);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 2 passed: Scale-down with minimal disruption");
    }

    // ==================== SCENARIO 3: Task Count Increase ====================
    
    /**
     * Scenario 3: Task count increased (10→15 tasks)
     * Expected Flow:
     * - Round 1: SKIPPED (no empty workers)
     * - Round 2: Assign 5 new tasks incrementally
     * 
     * Key: Existing 10 tasks preserved, only 5 new tasks assigned!
     */
    @Test
    public void testScenario3_TaskCountIncrease() {
        log.info("=== SCENARIO 3: Task Count Increase (10→15 tasks) ===");
        
        // Start with 3 workers, 10 tasks, balanced
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        addNewConnector("connector-1", 10);
        performStandardRebalance();
        performStandardRebalance();
        
        log.info("Initial state (3 workers, 10 tasks):");
        logAllAssignments();
        assertBalancedDistribution(3, 4);
        
        // Track existing tasks
        Set<ConnectorTaskId> existingTasks = new HashSet<>();
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            existingTasks.addAll(assignment.tasks());
        }
        assertEquals(10, existingTasks.size());
        
        // Increase task count to 15 (5 new tasks)
        connectors.put("connector-1", 15);
        
        log.info("After increasing connector-1 to 15 tasks:");
        
        // Round 2: Assign 5 new tasks only
        performStandardRebalance();
        
        log.info("After Round 2 (Incremental Assignment):");
        logAllAssignments();
        
        // Verify Round 2 only: 5 new tasks assigned, no revocations
        assertRound2Behavior(5, 0);
        
        // Verify all original 10 tasks are still assigned
        Set<ConnectorTaskId> currentTasks = new HashSet<>();
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            currentTasks.addAll(assignment.tasks());
        }
        assertTrue("All original tasks should be preserved", 
                currentTasks.containsAll(existingTasks));
        
        // Verify final balance: 5 tasks per worker (15 tasks / 3 workers)
        assertBalancedDistribution(5, 5);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 3 passed: Task count increase with preservation");
    }

    // ==================== SCENARIO 4: New Connector Added ====================
    
    /**
     * Scenario 4a: New connector added (no empty workers created)
     * Expected Flow:
     * - Round 1: SKIPPED (no empty workers)
     * - Round 2: Assign new connector's tasks
     */
    @Test
    public void testScenario4a_NewConnectorAdded_NoEmptyWorkers() {
        log.info("=== SCENARIO 4a: New Connector Added (no empty workers) ===");
        
        // Start with 3 workers, 1 connector with 10 tasks
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        addNewConnector("connector-1", 10);
        performStandardRebalance();
        performStandardRebalance();
        
        log.info("Initial state (1 connector, 10 tasks):");
        logAllAssignments();
        
        // Add new connector with 5 tasks
        addNewConnector("connector-2", 5);
        
        log.info("After adding connector-2 with 5 tasks:");
        
        // Round 2: Assign 5 new tasks
        performStandardRebalance();
        
        log.info("After Round 2 (Assignment):");
        logAllAssignments();
        
        // Verify Round 2 only: 5 tasks assigned, no revocations
        assertRound2Behavior(5, 0);
        
        // Verify final balance: 5 tasks per worker (15 tasks / 3 workers)
        assertBalancedDistribution(5, 5);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 4a passed: New connector added incrementally");
    }

    // ==================== SCENARIO 5: Connector Removed ====================
    
    /**
     * Scenario 5: Connector removed
     * Expected Flow:
     * - Tasks from deleted connector automatically filtered out
     * - Round 1: SKIPPED (no empty workers after removal)
     * - Round 2: Rebalance remaining tasks if needed
     */
    @Test
    public void testScenario5_ConnectorRemoved() {
        log.info("=== SCENARIO 5: Connector Removed ===");
        
        // Start with 3 workers, 2 connectors
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        addNewConnector("connector-1", 10);
        addNewConnector("connector-2", 5);
        performStandardRebalance();
        performStandardRebalance();
        
        log.info("Initial state (2 connectors, 15 tasks):");
        logAllAssignments();
        assertBalancedDistribution(5, 5);
        
        // Remove connector-2 (5 tasks)
        connectors.remove("connector-2");
        
        log.info("After removing connector-2:");
        
        // Tasks should be automatically filtered out
        performStandardRebalance();
        
        log.info("After rebalance:");
        logAllAssignments();
        
        // Verify only connector-1 tasks remain
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            for (ConnectorTaskId task : assignment.tasks()) {
                assertEquals("Only connector-1 tasks should remain", 
                        "connector-1", task.connector());
            }
        }
        
        // Verify final balance: 3-4 tasks per worker (10 tasks / 3 workers)
        assertBalancedDistribution(3, 4);
        
        // Total tasks should be 10 (connector-2's 5 tasks removed)
        int totalTasks = memberAssignments.values().stream()
                .mapToInt(a -> a.tasks().size())
                .sum();
        assertEquals(10, totalTasks);
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 5 passed: Connector removed and tasks filtered");
    }

    // ==================== SCENARIO 6: Task Count Decrease ====================
    
    /**
     * Scenario 6: Task count decreased (10→6 tasks)
     * Expected Flow:
     * - Tasks with removed IDs automatically filtered out
     * - Round 1: SKIPPED
     * - Round 2: Rebalance remaining 6 tasks
     */
    @Test
    public void testScenario6_TaskCountDecrease() {
        log.info("=== SCENARIO 6: Task Count Decrease (10→6 tasks) ===");
        
        // Start with 3 workers, 10 tasks
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        addNewConnector("connector-1", 10);
        performStandardRebalance();
        performStandardRebalance();
        
        log.info("Initial state (3 workers, 10 tasks):");
        logAllAssignments();
        assertBalancedDistribution(3, 4);
        
        // Decrease task count to 6 (tasks 6-9 removed)
        connectors.put("connector-1", 6);
        
        log.info("After decreasing connector-1 to 6 tasks:");
        
        // Round 1: Revoke all tasks (severe imbalance detected)
        performStandardRebalance();
        
        log.info("After Round 1 (Revocation):");
        logAllAssignments();
        
        // Round 2: Assign 6 tasks
        performStandardRebalance();
        
        log.info("After Round 2 (Assignment):");
        logAllAssignments();
        
        // Verify only tasks 0-5 remain
        Set<Integer> validTaskIds = new HashSet<>();
        for (int i = 0; i < 6; i++) {
            validTaskIds.add(i);
        }
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            for (ConnectorTaskId task : assignment.tasks()) {
                assertTrue("Only tasks 0-5 should remain, found: " + task.task(),
                        validTaskIds.contains(task.task()));
            }
        }
        
        // Verify final balance: 2 tasks per worker (6 tasks / 3 workers)
        assertBalancedDistribution(2, 2);
        
        // Total tasks should be 6
        int totalTasks = memberAssignments.values().stream()
                .mapToInt(a -> a.tasks().size())
                .sum();
        assertEquals(6, totalTasks);
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 6 passed: Task count decrease handled");
    }

    // ==================== SCENARIO 7: Severe Imbalance ====================
    
    /**
     * Scenario 7: Severe imbalance triggering Round 1
     * Expected Flow:
     * - Round 1: Full revocation (worker exceeds globalMaxLimit)
     * - Round 2: Complete redistribution
     */
    @Test
    public void testScenario7_SevereImbalanceTriggeringRound1() {
        log.info("=== SCENARIO 7: Severe Imbalance ===");
        
        // Setup: 3 workers with severe imbalance
        // worker1: 8 tasks (way over limit)
        // worker2: 1 task
        // worker3: 1 task
        // Total: 10 tasks, should be 3-4 each, limit should be ~4-5
        
        addNewConnector("connector-1", 10);
        
        List<ConnectorTaskId> worker1Tasks = createTaskIds("connector-1", 0, 1, 2, 3, 4, 5, 6, 7);
        List<ConnectorTaskId> worker2Tasks = createTaskIds("connector-1", 8);
        List<ConnectorTaskId> worker3Tasks = createTaskIds("connector-1", 9);
        
        memberAssignments.put("worker1", new ConnectorsAndTasks.Builder()
                .with(Collections.singletonList("connector-1"), worker1Tasks).build());
        memberAssignments.put("worker2", new ConnectorsAndTasks.Builder()
                .with(Collections.emptyList(), worker2Tasks).build());
        memberAssignments.put("worker3", new ConnectorsAndTasks.Builder()
                .with(Collections.emptyList(), worker3Tasks).build());
        
        log.info("Initial imbalanced state:");
        logAllAssignments();
        
        // Round 1: Should trigger full revocation
        performStandardRebalance();
        
        log.info("After Round 1 (Revocation):");
        logAllAssignments();
        
        // Verify Round 1: All 10 tasks revoked
        assertRound1Behavior(10, 0);
        
        // Round 2: Redistribute all tasks
        performStandardRebalance();
        
        log.info("After Round 2 (Assignment):");
        logAllAssignments();
        
        // Verify Round 2: All 10 tasks assigned
        assertRound2Behavior(10, 0);
        
        // Verify final balance: 3-4 tasks per worker
        assertBalancedDistribution(3, 4);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 7 passed: Severe imbalance corrected via Round 1");
    }

    // ==================== Multi-Connector Scenarios ====================
    
    /**
     * Scenario 8: Multi-connector balanced distribution
     * Tests per-consumer fairness with multiple connectors
     */
    @Test
    public void testScenario8_MultiConnectorBalance() {
        log.info("=== SCENARIO 8: Multi-Connector Balance ===");
        
        // 3 workers, 3 connectors with different task counts
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        addNewConnector("connector-1", 9);   // 3 per worker
        addNewConnector("connector-2", 6);   // 2 per worker
        addNewConnector("connector-3", 3);   // 1 per worker
        
        performStandardRebalance();
        performStandardRebalance();
        
        log.info("Final state (3 connectors, 18 tasks):");
        logAllAssignments();
        
        // Verify global balance: 6 tasks per worker
        assertBalancedDistribution(6, 6);
        
        // Verify per-connector balance
        for (String worker : memberAssignments.keySet()) {
            ConnectorsAndTasks assignment = memberAssignments.get(worker);
            
            long c1Count = assignment.tasks().stream()
                    .filter(t -> t.connector().equals("connector-1")).count();
            long c2Count = assignment.tasks().stream()
                    .filter(t -> t.connector().equals("connector-2")).count();
            long c3Count = assignment.tasks().stream()
                    .filter(t -> t.connector().equals("connector-3")).count();
            
            assertEquals("Each worker should have 3 connector-1 tasks", 3, c1Count);
            assertEquals("Each worker should have 2 connector-2 tasks", 2, c2Count);
            assertEquals("Each worker should have 1 connector-3 task", 1, c3Count);
        }
        
        assertAllTasksAssigned();
        assertNoDuplicateConnectorAssignments();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 8 passed: Multi-connector per-consumer fairness");
    }

    // ==================== Helper Methods ====================

    /**
     * Verify Round 1 behavior: Only revocations, no assignments
     */
    private void assertRound1Behavior(int expectedRevocations, int expectedAssignments) {
        assertEquals("Round 1 should have exactly " + expectedRevocations + " revocations",
                expectedRevocations, countTotalRevocations());
        assertEquals("Round 1 should have exactly " + expectedAssignments + " assignments (typically 0)",
                expectedAssignments, countTotalAssignments());
    }
    
    /**
     * Verify Round 2 behavior: Only assignments, no revocations
     */
    private void assertRound2Behavior(int expectedAssignments, int expectedRevocations) {
        assertEquals("Round 2 should have exactly " + expectedAssignments + " assignments",
                expectedAssignments, countTotalAssignments());
        assertEquals("Round 2 should have exactly " + expectedRevocations + " revocations (typically 0)",
                expectedRevocations, countTotalRevocations());
    }
    
    /**
     * Count total revocations in the last ClusterAssignment (tasks only)
     */
    private int countTotalRevocations() {
        if (returnedAssignments == null) {
            return 0;
        }
        int total = 0;
        for (String worker : returnedAssignments.allWorkers()) {
            // Only count task revocations, not connector revocations
            total += returnedAssignments.newlyRevokedTasks(worker).size();
        }
        return total;
    }
    
    /**
     * Count total assignments in the last ClusterAssignment (tasks only)
     */
    private int countTotalAssignments() {
        if (returnedAssignments == null) {
            return 0;
        }
        int total = 0;
        for (String worker : returnedAssignments.allWorkers()) {
            // Only count task assignments, not connector assignments
            total += returnedAssignments.newlyAssignedTasks(worker).size();
        }
        return total;
    }
    
    /**
     * Verify balanced distribution within min/max range
     */
    private void assertBalancedDistribution(int minTasksPerWorker, int maxTasksPerWorker) {
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            int taskCount = entry.getValue().tasks().size();
            assertTrue(
                    String.format("Worker %s has %d tasks, expected [%d, %d]",
                            entry.getKey(), taskCount, minTasksPerWorker, maxTasksPerWorker),
                    taskCount >= minTasksPerWorker && taskCount <= maxTasksPerWorker
            );
        }
    }

    private void performStandardRebalance() {
        performRebalance(false);
    }

    private void performRebalance(boolean expectFailure) {
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
            if (!expectFailure) {
                throw e;
            }
            return;
        }
        
        if (!expectFailure) {
            applyAssignments();
        }
    }

    private void applyAssignments() {
        returnedAssignments.allWorkers().forEach(worker -> {
            ConnectorsAndTasks current = memberAssignments.getOrDefault(
                    worker, 
                    ConnectorsAndTasks.EMPTY);

            // Build new assignment: current - revoked + assigned
            Set<String> newConnectors = new HashSet<>(current.connectors());
            newConnectors.removeAll(returnedAssignments.newlyRevokedConnectors(worker));
            newConnectors.addAll(returnedAssignments.newlyAssignedConnectors(worker));
            
            Set<ConnectorTaskId> newTasks = new HashSet<>(current.tasks());
            newTasks.removeAll(returnedAssignments.newlyRevokedTasks(worker));
            newTasks.addAll(returnedAssignments.newlyAssignedTasks(worker));
            
            memberAssignments.put(worker, new ConnectorsAndTasks.Builder()
                    .with(newConnectors, newTasks)
                    .build());
        });
    }

    private void addNewEmptyWorkers(String... workers) {
        for (String worker : workers) {
            memberAssignments.put(worker, new ConnectorsAndTasks.Builder().build());
        }
    }

    private void removeWorkers(String... workers) {
        for (String worker : workers) {
            memberAssignments.remove(worker);
        }
    }

    private void addNewConnector(String connector, int taskCount) {
        connectors.put(connector, taskCount);
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

    private List<ConnectorTaskId> createTaskIds(String connector, int... taskNumbers) {
        List<ConnectorTaskId> tasks = new ArrayList<>();
        for (int taskNum : taskNumbers) {
            tasks.add(new ConnectorTaskId(connector, taskNum));
        }
        return tasks;
    }

    private void assertAllTasksAssigned() {
        Set<ConnectorTaskId> assignedTasks = new HashSet<>();
        List<ConnectorTaskId> allTasksList = new ArrayList<>();
        
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            allTasksList.addAll(assignment.tasks());
            assignedTasks.addAll(assignment.tasks());
        }
        
        // Check for duplicate task assignments
        if (allTasksList.size() != assignedTasks.size()) {
            Map<ConnectorTaskId, Long> taskCounts = allTasksList.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            List<String> duplicates = taskCounts.entrySet().stream()
                    .filter(e -> e.getValue() > 1)
                    .map(e -> e.getKey() + " assigned " + e.getValue() + " times")
                    .collect(Collectors.toList());
            fail("Found duplicate task assignments: " + String.join(", ", duplicates));
        }
        
        int expectedTasks = connectors.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals("Not all tasks are assigned", expectedTasks, assignedTasks.size());
    }

    private void assertNoDuplicateConnectorAssignments() {
        Map<String, List<String>> connectorToWorkers = new HashMap<>();
        
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            String worker = entry.getKey();
            for (String connector : entry.getValue().connectors()) {
                connectorToWorkers.computeIfAbsent(connector, k -> new ArrayList<>()).add(worker);
            }
        }
        
        List<String> duplicates = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : connectorToWorkers.entrySet()) {
            if (entry.getValue().size() > 1) {
                duplicates.add(entry.getKey() + " assigned to " + entry.getValue());
            }
        }
        
        if (!duplicates.isEmpty()) {
            fail("Found duplicate connector assignments: " + String.join(", ", duplicates));
        }
    }

    private void assertAllConnectorsAssigned() {
        Set<String> assignedConnectors = new HashSet<>();
        List<String> allConnectorsList = new ArrayList<>();
        
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            allConnectorsList.addAll(assignment.connectors());
            assignedConnectors.addAll(assignment.connectors());
        }
        
        // Check for duplicate connector assignments
        if (allConnectorsList.size() != assignedConnectors.size()) {
            Map<String, Long> connectorCounts = allConnectorsList.stream()
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            List<String> duplicates = connectorCounts.entrySet().stream()
                    .filter(e -> e.getValue() > 1)
                    .map(e -> e.getKey() + " assigned " + e.getValue() + " times")
                    .collect(Collectors.toList());
            fail("Found duplicate connector assignments: " + String.join(", ", duplicates));
        }
        
        int expectedConnectors = connectors.size();
        assertEquals("Not all connectors are assigned", expectedConnectors, assignedConnectors.size());
        
        // Verify each expected connector is assigned
        for (String connector : connectors.keySet()) {
            assertTrue("Connector " + connector + " is not assigned", assignedConnectors.contains(connector));
        }
    }

    private void assertConnectorsEvenlyDistributed() {
        if (memberAssignments.isEmpty() || connectors.isEmpty()) {
            return; // Nothing to verify
        }
        
        // Count connectors per worker
        Map<String, Integer> connectorsPerWorker = new HashMap<>();
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            connectorsPerWorker.put(entry.getKey(), entry.getValue().connectors().size());
        }
        
        int totalConnectors = connectors.size();
        int numWorkers = memberAssignments.size();
        int minExpected = totalConnectors / numWorkers;
        int maxExpected = (totalConnectors + numWorkers - 1) / numWorkers; // Ceiling division
        
        log.info("Connector distribution check: {} connectors across {} workers (expected range: {}-{})",
                totalConnectors, numWorkers, minExpected, maxExpected);
        
        for (Map.Entry<String, Integer> entry : connectorsPerWorker.entrySet()) {
            int count = entry.getValue();
            log.info("  {}: {} connectors", entry.getKey(), count);
            
            if (count < minExpected || count > maxExpected) {
                fail(String.format("Worker %s has %d connectors, expected range [%d, %d]. Distribution: %s",
                        entry.getKey(), count, minExpected, maxExpected, connectorsPerWorker));
            }
        }
    }

    private void logAllAssignments() {
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            log.info("  {}: connectors={}, tasks={}", 
                    entry.getKey(), 
                    entry.getValue().connectors(),
                    entry.getValue().tasks().stream()
                            .map(t -> t.connector() + "-" + t.task())
                            .collect(Collectors.toList()));
        }
    }
}

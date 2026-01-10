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
 * Tests verify the 4-step incremental cooperative protocol:
 * - Step 1 (ALWAYS): Calculate balance targets
 * - Step 2 (ALWAYS): Detect violations (global and per-consumer)
 * - Step 3 (CONDITIONAL): Revoke from overloaded workers (if overload violations detected)
 * - Step 4 (CONDITIONAL): Assign to underloaded workers or unassigned tasks (if no overload violations)
 * 
 * Key Protocol Rule: Steps 3 and 4 are MUTUALLY EXCLUSIVE - only one executes per generation.
 * 
 * Scenarios tested (from algorithm specification):
 * 1. Scale-up (empty workers) → Assignment generation (no overload, just unassigned tasks)
 * 2. Scale-down (workers left) → Assignment generation (no overload, just unassigned tasks)
 * 3. Task count increase → Assignment generation (no overload, just new tasks)
 * 4. New connector added → Assignment generation (no overload, just new tasks)
 * 5. Connector removed → Assignment generation (tasks filtered, may rebalance remaining)
 * 6. Task count decrease → No action or minor rebalancing (tasks filtered out)
 * 7. Overload violation → Revocation generation (remove excess tasks)
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
     * Expected Flow (NEW ALGORITHM):
     * - Generation 1: No overload violations (all workers empty), but unassigned tasks exist
     *   → Step 4 (Assignment): Assign all 10 tasks
     * 
     * This tests the classic scale-up scenario where new workers join an empty cluster.
     * No revocations occur since there's no overload.
     */
    @Test
    public void testScenario1_ScaleUpFrom0To3Workers() {
        log.info("=== SCENARIO 1: Scale-Up (0→3 workers) ===");
        
        // Start with 3 empty workers
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        
        // Add 1 connector with 10 tasks
        addNewConnector("connector-1", 10);
        
        // Generation 1: All tasks unassigned, no overload violations
        // → Step 4 (Assignment) executes: assign all 10 tasks
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify: All 10 tasks assigned in single generation, no revocations
        assertAssignmentBehavior(10, 0);  // 10 assignments, 0 revocations
        
        // Verify final balance: 3-4 tasks per worker (10/3 = 3.33, so min=3, max=4)
        assertBalancedDistribution(3, 4);
        assertAllTasksAssigned();
        assertNoDuplicateConnectorAssignments();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 1 passed: Scale-up handled correctly in single generation");
    }
    
    /**
     * Scenario 1b: Scale-Up from 2 to 5 workers (with existing load)
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 2 workers, balanced with 5 tasks each
     * - Add 3 empty workers → creates underload violation (new workers have 0 < globalMin=2)
     * - Generation 1: Detect global underload violations for new workers
     *   → Step 4 (Assignment): Incrementally assign tasks to fill underloaded workers
     * 
     * Key: The algorithm does NOT revoke all tasks. It detects that the 3 new workers
     * are underloaded and incrementally assigns tasks to balance the load.
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
        
        // Generation 1: New workers are underloaded (0 < globalMin=2)
        // → Step 4 (Assignment): Assign tasks to fill underloaded workers
        // But wait - existing workers are now OVERLOADED (5 > globalMaxLimit=2+1=3)
        // → Step 3 (Revocation): Revoke excess tasks from overloaded workers
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 3 - Revocation):");
        logAllAssignments();
        
        // Verify Generation 1: Revocations from overloaded workers, no assignments yet
        int revocations = countTotalRevocations();
        int assignments = countTotalAssignments();
        assertTrue("Generation 1 should have revocations", revocations > 0);
        assertEquals("Generation 1 should have NO assignments", 0, assignments);
        
        // Generation 2: Revoked tasks now unassigned, no overload violations
        // → Step 4 (Assignment): Assign unassigned tasks to balance all workers
        performStandardRebalance();
        
        log.info("After Generation 2 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify Generation 2: Assignments only, no revocations
        revocations = countTotalRevocations();
        assignments = countTotalAssignments();
        assertEquals("Generation 2 should have NO revocations", 0, revocations);
        assertTrue("Generation 2 should have assignments", assignments > 0);
        
        // Verify final balance: 2 tasks per worker (10 tasks / 5 workers)
        assertBalancedDistribution(2, 2);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 1b passed: Scale-up with existing load handled via incremental revocation→assignment");
    }

    // ==================== SCENARIO 2: Scale-Down (Workers Left) ====================
    
    /**
     * Scenario 2: Scale-Down from 5 to 3 workers
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 5 workers, 10 tasks, balanced (2 per worker)
     * - Remove 2 workers → 4 tasks become unassigned, remaining 3 workers still have their 2 tasks
     * - Generation 1: No overload violations (3 workers with 2 tasks each is valid for 6 remaining tasks after losing 4)
     *   But wait - we still have 10 total tasks in the system, 4 are unassigned, so 6 tasks across 3 workers = 2 per worker
     *   globalMin = 10 / 3 = 3 (floor), globalMax = 4 (ceil)
     *   Current state: 3 workers with 2 tasks each = UNDERLOAD (2 < globalMin=3)
     *   → Step 4 (Assignment): Assign the 4 lost tasks to the 3 remaining workers to reach balance
     * 
     * Key: The algorithm does NOT revoke existing tasks. It only assigns the lost tasks.
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
        
        // Generation 1: 3 workers with 2 tasks each, 4 unassigned tasks
        // globalMin = 10/3 = 3, globalMax = 4
        // All workers underloaded (2 < globalMin=3)
        // → Step 4 (Assignment): Assign lost tasks to balance
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify Generation 1: Assignments only, no revocations
        assertAssignmentBehavior(lostTaskCount, 0);
        
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
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 3 workers, 10 tasks, balanced (3-4 per worker)
     * - Increase to 15 tasks → 5 new unassigned tasks
     * - Generation 1: No overload violations (workers have 3-4 tasks each)
     *   globalMin = 15/3 = 5, globalMax = 5
     *   Current state: Workers underloaded (3-4 < globalMin=5)
     *   → Step 4 (Assignment): Assign the 5 new tasks to reach balance
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
        
        // Generation 1: 3 workers underloaded, 5 unassigned tasks
        // → Step 4 (Assignment): Assign 5 new tasks only
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify Generation 1: 5 new tasks assigned, no revocations
        assertAssignmentBehavior(5, 0);
        
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
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 3 workers, 10 tasks from connector-1, balanced (3-4 per worker)
     * - Add connector-2 with 5 tasks → 5 new unassigned tasks
     * - Generation 1: No overload violations (workers have 3-4 tasks each)
     *   globalMin = 15/3 = 5, globalMax = 5
     *   Current state: Workers underloaded (3-4 < globalMin=5)
     *   → Step 4 (Assignment): Assign the 5 new tasks
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
        
        // Generation 1: No overload, 5 unassigned tasks
        // → Step 4 (Assignment): Assign 5 new tasks
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify Generation 1: 5 tasks assigned, no revocations
        assertAssignmentBehavior(5, 0);
        
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
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 3 workers, 2 connectors (15 tasks total), balanced (5 per worker)
     * - Remove connector-2 → 5 tasks automatically filtered out, 10 tasks remain
     * - Generation 1: No action needed if workers already have 3-4 tasks each after filtering
     *   If filtering leaves workers with exactly their assigned tasks from connector-1, likely balanced already
     * 
     * Note: This scenario tests the connector filtering mechanism, not rebalancing logic
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
    /**
     * Scenario 6: Task count decreased (10→6 tasks)
     * Expected Flow (NEW ALGORITHM):
     * - Initial: 3 workers, 10 tasks, balanced (3-4 per worker)
     * - Decrease to 6 tasks → tasks 6-9 automatically filtered out
     * - Generation 1: After filtering, workers may have 2-3 tasks each
     *   globalMin = 6/3 = 2, globalMax = 2
     *   If some workers have 3 tasks: 3 > globalMaxLimit=3, NO overload
     *   But some tasks might still be assigned to "deleted" task IDs
     *   The filtering will remove tasks 6-9, possibly creating imbalance
     * 
     * This scenario may trigger rebalancing depending on how tasks were distributed.
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
        
        // Generation 1: Tasks 6-9 filtered, may need rebalancing
        performStandardRebalance();
        
        log.info("After Generation 1:");
        logAllAssignments();
        
        // May need Generation 2 if filtering caused imbalance
        performStandardRebalance();
        
        log.info("After Generation 2:");
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
     * Scenario 7: Overload violation triggering Step 3 (Revocation Generation)
     * Expected Flow (NEW ALGORITHM):
     * - Initial: Severe imbalance - worker1 has 8 tasks, worker2/3 have 1 each
     * - Generation 1: Detect overload violation (worker1 has 8 > globalMaxLimit)
     *   globalMin = 10/3 = 3, globalMax = 4, globalMaxLimit = 3+1 = 4
     *   worker1: 8 tasks (OVERLOAD: 8 > 4)
     *   → Step 3 (Revocation): Revoke excess tasks from worker1 only
     * - Generation 2: No overload, but unassigned tasks and underload violations
     *   → Step 4 (Assignment): Assign revoked tasks to balance all workers
     */
    @Test
    public void testScenario7_OverloadTriggeringRevocation() {
        log.info("=== SCENARIO 7: Overload Violation ===");
        
        // Setup: 3 workers with severe imbalance
        // worker1: 8 tasks (way over limit)
        // worker2: 1 task
        // worker3: 1 task
        // Total: 10 tasks, globalMin=3, globalMax=4, globalMaxLimit=4
        
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
        
        // Generation 1: worker1 overloaded (8 > globalMaxLimit=4)
        // → Step 3 (Revocation): Revoke excess tasks from worker1
        performStandardRebalance();
        
        log.info("After Generation 1 (Step 3 - Revocation):");
        logAllAssignments();
        
        // Verify Generation 1: Revocations only (at least 4 tasks revoked from worker1)
        int revocations = countTotalRevocations();
        int assignments = countTotalAssignments();
        assertTrue("Generation 1 should revoke tasks", revocations >= 4);
        assertEquals("Generation 1 should have NO assignments", 0, assignments);
        
        // Generation 2: No overload, revoked tasks unassigned
        // → Step 4 (Assignment): Assign all unassigned tasks to balance
        performStandardRebalance();
        
        log.info("After Generation 2 (Step 4 - Assignment):");
        logAllAssignments();
        
        // Verify Generation 2: Assignments only
        revocations = countTotalRevocations();
        assignments = countTotalAssignments();
        assertEquals("Generation 2 should have NO revocations", 0, revocations);
        assertTrue("Generation 2 should assign tasks", assignments >= 4);
        
        // Verify final balance: 3-4 tasks per worker
        assertBalancedDistribution(3, 4);
        assertAllTasksAssigned();
        assertAllConnectorsAssigned();
        assertConnectorsEvenlyDistributed();
        
        log.info("✓ SCENARIO 7 passed: Overload corrected via Step 3 Revocation → Step 4 Assignment");
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
    
    /**
     * Verify Step 4 (Assignment Generation) behavior: Only assignments, no revocations
     * This generation resolves underload violations by assigning tasks to underloaded workers.
     */
    private void assertAssignmentBehavior(int expectedAssignments, int expectedRevocations) {
        assertEquals("Step 4 (Assignment) should have exactly " + expectedAssignments + " assignments",
                expectedAssignments, countTotalAssignments());
        assertEquals("Step 4 (Assignment) should have exactly " + expectedRevocations + " revocations (always 0)",
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

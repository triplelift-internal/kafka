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
 * Simple tests for BalancedCooperativeAssignor to identify root causes of failures.
 * 
 * This test class uses minimal scenarios (3 workers, 1 connector, 10 tasks) to isolate
 * and debug issues with the balanced cooperative rebalancing algorithm.
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
        
        // Start with 3 workers for simple testing
        addNewEmptyWorkers("worker1", "worker2", "worker3");
        initAssignor();
    }

    private void initAssignor() {
        assignor = new BalancedCooperativeAssignor(logContext, time, rebalanceDelay);
        assignor.previousGenerationId = generationId;
    }

    /**
     * Test 1: Simple initial assignment
     * - 3 workers, all empty
     * - 1 connector with 10 tasks
     * - Expected: Each worker gets 3-4 tasks (balanced)
     */
    @Test
    public void testSimpleInitialAssignment() {
        log.info("=== Test 1: Simple Initial Assignment ===");
        
        // Add 1 connector with 10 tasks
        addNewConnector("C1-connector1", 10);
        
        // Round 1: Initial assignment
        performStandardRebalance();
        
        log.info("After Round 1:");
        logAllAssignments();
        
        // Verify no duplicate connectors
        assertNoDuplicateConnectorAssignments();
        
        // Verify all tasks assigned
        assertAllTasksAssigned();
        
        // Verify balanced (each worker should have 3-4 tasks)
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            int taskCount = entry.getValue().tasks().size();
            assertTrue("Worker " + entry.getKey() + " has " + taskCount + " tasks, expected 3-4",
                    taskCount >= 3 && taskCount <= 4);
        }
        
        log.info("✓ Test 1 passed: Simple initial assignment works correctly");
    }

    /**
     * Test 2: Rebalance from imbalanced state
     * - 3 workers: worker1=7, worker2=3, worker3=0
     * - 1 connector with 10 tasks total
     * - Expected: Converge to balanced state (3-4 tasks each) within a few rounds
     */
    @Test
    public void testRebalanceFromImbalancedState() {
        log.info("=== Test 2: Rebalance from Imbalanced State ===");
        
        // Add connector
        addNewConnector("C1-connector1", 10);
        
        // Setup imbalanced initial state
        List<ConnectorTaskId> worker1Tasks = createTaskIds("C1-connector1", 0, 1, 2, 3, 4, 5, 6);
        List<ConnectorTaskId> worker2Tasks = createTaskIds("C1-connector1", 7, 8, 9);
        
        // Only worker1 gets the connector (connectors are not split, only tasks)
        memberAssignments.put("worker1", new ConnectorsAndTasks.Builder()
                .with(Collections.singletonList("C1-connector1"), worker1Tasks).build());
        memberAssignments.put("worker2", new ConnectorsAndTasks.Builder()
                .with(Collections.emptyList(), worker2Tasks).build());
        memberAssignments.put("worker3", new ConnectorsAndTasks.Builder()
                .with(Collections.emptyList(), Collections.emptyList()).build());
        
        log.info("Initial imbalanced state:");
        logAllAssignments();
        
        // Converge through multiple rounds
        int maxRounds = 6;
        for (int round = 1; round <= maxRounds; round++) {
            performStandardRebalance();
            
            log.info("After Round {}:", round);
            logAllAssignments();
            
            // Verify no duplicates after each round
            assertNoDuplicateConnectorAssignments();
            
            // Check if balanced
            if (isBalanced()) {
                log.info("✓ Test 2 passed: Converged to balanced state in {} rounds", round);
                assertAllTasksAssigned();
                return;
            }
        }
        
        fail("Failed to converge to balanced state after " + maxRounds + " rounds");
    }

    /**
     * Test 3: Worker leaving scenario
     * - Start: 3 workers with balanced assignment
     * - Remove worker3
     * - Expected: Tasks redistributed to worker1 and worker2
     */
    @Test
    public void testWorkerLeaving() {
        log.info("=== Test 3: Worker Leaving ===");
        
        // Add connector and do initial assignment
        addNewConnector("C1-connector1", 10);
        performStandardRebalance();
        
        log.info("Initial balanced state:");
        logAllAssignments();
        assertNoDuplicateConnectorAssignments();
        
        // Remove worker3
        removeWorkers("worker3");
        
        log.info("After removing worker3:");
        logAllAssignments();
        
        // Rebalance
        performStandardRebalance();
        
        log.info("After rebalancing:");
        logAllAssignments();
        
        // Verify
        assertNoDuplicateConnectorAssignments();
        assertAllTasksAssigned();
        
        // Should be balanced between 2 workers (5 tasks each)
        assertEquals(5, memberAssignments.get("worker1").tasks().size());
        assertEquals(5, memberAssignments.get("worker2").tasks().size());
        
        log.info("✓ Test 3 passed: Worker leaving handled correctly");
    }

    /**
     * Test 4: Worker joining scenario
     * - Start: 2 workers with balanced assignment (5 tasks each)
     * - Add worker3
     * - Expected: Tasks redistributed to all 3 workers (3-4 each)
     */
    @Test
    public void testWorkerJoining() {
        log.info("=== Test 4: Worker Joining ===");
        
        // Start with only 2 workers
        removeWorkers("worker3");
        
        // Add connector and do initial assignment
        addNewConnector("C1-connector1", 10);
        performStandardRebalance();
        
        log.info("Initial state with 2 workers:");
        logAllAssignments();
        assertNoDuplicateConnectorAssignments();
        
        // Add worker3
        addNewEmptyWorkers("worker3");
        
        log.info("After adding worker3:");
        logAllAssignments();
        
        // Rebalance (may take multiple rounds for cooperative protocol)
        int maxRounds = 6;
        for (int round = 1; round <= maxRounds; round++) {
            performStandardRebalance();
            
            log.info("After rebalance round {}:", round);
            logAllAssignments();
            
            assertNoDuplicateConnectorAssignments();
            
            // Check if all tasks assigned and balanced
            try {
                assertAllTasksAssigned();
                if (isBalanced()) {
                    log.info("✓ Test 4 passed: Worker joining handled correctly in {} rounds", round);
                    return;
                }
            } catch (AssertionError e) {
                if (round == maxRounds) {
                    throw e;
                }
                log.debug("Round {}: Not yet fully assigned/balanced", round);
            }
        }
        
        // Final verification
        assertAllTasksAssigned();
        
        // Should be balanced across 3 workers (3-4 tasks each)
        for (Map.Entry<String, ConnectorsAndTasks> entry : memberAssignments.entrySet()) {
            int taskCount = entry.getValue().tasks().size();
            assertTrue("Worker " + entry.getKey() + " has " + taskCount + " tasks, expected 3-4",
                    taskCount >= 3 && taskCount <= 4);
        }
        
        log.info("✓ Test 4 passed: Worker joining handled correctly");
    }

    // ==================== Helper Methods ====================

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

    private boolean isBalanced() {
        if (memberAssignments.isEmpty()) {
            return true;
        }
        
        int minTasks = Integer.MAX_VALUE;
        int maxTasks = 0;
        
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            int taskCount = assignment.tasks().size();
            minTasks = Math.min(minTasks, taskCount);
            maxTasks = Math.max(maxTasks, taskCount);
        }
        
        return (maxTasks - minTasks) <= 1;
    }

    private void assertAllTasksAssigned() {
        Set<ConnectorTaskId> assignedTasks = new HashSet<>();
        for (ConnectorsAndTasks assignment : memberAssignments.values()) {
            assignedTasks.addAll(assignment.tasks());
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

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
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for GlobalBalanceAssignor functionality
 * Tests the core requirement: maximum difference of 1 task between workers
 */
public class GlobalBalanceAssignorTest {

    private GlobalBalanceAssignor assignor;

    @Before
    public void setup() {
        MockTime time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
    }

    /**
     * Test basic task assignment functionality
     */
    @Test
    public void testBasicTaskAssignment() {
        // Create workers
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker1").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker2").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker3").withCopies(new ArrayList<>(), new ArrayList<>()).build()
        );

        // Create tasks for one connector
        List<ConnectorTaskId> tasks = Arrays.asList(
            new ConnectorTaskId("connector1", 0),
            new ConnectorTaskId("connector1", 1),
            new ConnectorTaskId("connector1", 2),
            new ConnectorTaskId("connector1", 3),
            new ConnectorTaskId("connector1", 4),
            new ConnectorTaskId("connector1", 5)
        );

        // Assign tasks
        assignor.assignTasks(workers, tasks);

        // Verify tasks are distributed evenly across workers
        assertEquals(2, workers.get(0).tasksSize());
        assertEquals(2, workers.get(1).tasksSize());
        assertEquals(2, workers.get(2).tasksSize());

        // Verify all tasks are assigned
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(6, totalAssigned);
    }

    /**
     * Test multiple connector task assignment functionality  
     */
    @Test
    public void testMultipleConnectorTaskAssignment() {
        // Create workers
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker1").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker2").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker3").withCopies(new ArrayList<>(), new ArrayList<>()).build()
        );

        // Create tasks for multiple connectors
        List<ConnectorTaskId> tasks = Arrays.asList(
            new ConnectorTaskId("connector1", 0),
            new ConnectorTaskId("connector1", 1),
            new ConnectorTaskId("connector1", 2),
            new ConnectorTaskId("connector2", 0),
            new ConnectorTaskId("connector2", 1),
            new ConnectorTaskId("connector2", 2)
        );

        // Assign tasks
        assignor.assignTasks(workers, tasks);

        // Verify tasks are distributed evenly across workers
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(6, totalAssigned);

        // Each worker should have tasks from both connectors distributed evenly
        for (WorkerLoad worker : workers) {
            assertTrue(worker.tasksSize() >= 1);
            assertTrue(worker.tasksSize() <= 3);
        }
    }

    /**
     * Test connector assignment functionality
     */
    @Test
    public void testConnectorAssignment() {
        // Create workers
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker1").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker2").withCopies(new ArrayList<>(), new ArrayList<>()).build(),
            new WorkerLoad.Builder("worker3").withCopies(new ArrayList<>(), new ArrayList<>()).build()
        );

        // Create connectors
        List<String> connectors = Arrays.asList("connector1", "connector2", "connector3", "connector4");

        // Assign connectors
        assignor.assignConnectors(workers, connectors);

        // Verify connectors are distributed evenly
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::connectorsSize).sum();
        assertEquals(4, totalAssigned);

        // Each worker should have at least one connector, with balanced distribution
        for (WorkerLoad worker : workers) {
            assertTrue("Each worker should have at least one connector", worker.connectorsSize() >= 1);
            assertTrue("No worker should have more than 2 connectors", worker.connectorsSize() <= 2);
        }
    }

    @Test
    public void testPerfectTaskBalanceRequirement() {
        // Test the critical requirement: maximum difference of 1 task between workers
        
        // Test case 1: 100 tasks, 3 workers (the main example from requirements)
        verifyPerfectBalance(100, 3);
        
        // Test case 2: Various scenarios  
        verifyPerfectBalance(7, 3);   // [2, 2, 3]
        verifyPerfectBalance(10, 4);  // [2, 2, 3, 3]
        verifyPerfectBalance(1, 5);   // [0, 0, 0, 0, 1]
    }

    private void verifyPerfectBalance(int taskCount, int workerCount) {
        // Create empty workers
        List<WorkerLoad> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            workers.add(new WorkerLoad.Builder("worker" + i).build());
        }

        // Create tasks
        List<ConnectorTaskId> tasks = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new ConnectorTaskId("connector" + (i % 3), i));
        }

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks assigned
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals("All tasks should be assigned", taskCount, totalAssigned);

        // Verify perfect balance constraint
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);
        int difference = maxTasks - minTasks;

        assertTrue(String.format("CRITICAL: Task balance violated! " +
                               "Tasks: %d, Workers: %d, Distribution: %s, " +
                               "Min: %d, Max: %d, Difference: %d (must be ≤ 1)",
                               taskCount, workerCount, Arrays.toString(taskCounts),
                               minTasks, maxTasks, difference), difference <= 1);
    }

    @Test
    public void testOriginalScenarioFailureAndReassignment() {
        // Simulate the exact scenario: W9 and W2 failed, need to reassign their tasks
        // Tasks to reassign: C1 C1 C2 C2 C1 C1 C2 C5 C9 (from W2 and W9)

        // Available workers after failure (W1, W3, W5, W6, W7, W8)
        List<WorkerLoad> workers = Arrays.asList(
            // Existing assignments before rebalance
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1), 
                             new ConnectorTaskId("C2", 0), new ConnectorTaskId("C6", 0))
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 2), new ConnectorTaskId("C2", 1), 
                             new ConnectorTaskId("C3", 0), new ConnectorTaskId("C7", 0))
            ).build(),
            new WorkerLoad.Builder("W5").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 3), new ConnectorTaskId("C2", 2), 
                             new ConnectorTaskId("C3", 1), new ConnectorTaskId("C7", 1))
            ).build(),
            new WorkerLoad.Builder("W6").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 4), new ConnectorTaskId("C2", 3), 
                             new ConnectorTaskId("C4", 0), new ConnectorTaskId("C8", 0))
            ).build(),
            new WorkerLoad.Builder("W7").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 5), new ConnectorTaskId("C2", 4), 
                             new ConnectorTaskId("C4", 1), new ConnectorTaskId("C9", 0))
            ).build(),
            new WorkerLoad.Builder("W8").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 6), new ConnectorTaskId("C2", 5), 
                             new ConnectorTaskId("C5", 0), new ConnectorTaskId("C9", 1))
            ).build()
        );

        // Tasks that need to be reassigned (from failed W2 and W9)
        List<ConnectorTaskId> tasksToReassign = Arrays.asList(
            // From W2: C1 C1 C2 C2 C1
            new ConnectorTaskId("C1", 10), new ConnectorTaskId("C1", 11),
            new ConnectorTaskId("C2", 10), new ConnectorTaskId("C2", 11),
            new ConnectorTaskId("C1", 12),
            // From W9: C1 C2 C5 C9
            new ConnectorTaskId("C1", 13), new ConnectorTaskId("C2", 12),
            new ConnectorTaskId("C5", 10), new ConnectorTaskId("C9", 10)
        );

        // Get initial task counts per worker
        int[] initialCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();

        // Perform global balance assignment
        assignor.assignTasks(workers, tasksToReassign);

        // Verify all tasks were assigned
        int totalTasksAfter = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        int totalTasksBefore = Arrays.stream(initialCounts).sum();
        assertEquals("All new tasks should be assigned", totalTasksBefore + tasksToReassign.size(), totalTasksAfter);

        // Verify global balance: tasks for same connector are spread across workers

        // Check C1 task distribution (should be spread across multiple workers)
        List<String> workersWithC1Tasks = workers.stream()
            .filter(w -> w.taskCountForConnector("C1") > 0)
            .map(WorkerLoad::worker)
            .collect(Collectors.toList());
        assertTrue("C1 tasks should be spread across at least 4 workers, but found in: " + workersWithC1Tasks,
                  workersWithC1Tasks.size() >= 4);

        // Check C2 task distribution (should be spread across multiple workers)
        List<String> workersWithC2Tasks = workers.stream()
            .filter(w -> w.taskCountForConnector("C2") > 0)
            .map(WorkerLoad::worker)
            .collect(Collectors.toList());
        assertTrue("C2 tasks should be spread across at least 4 workers, but found in: " + workersWithC2Tasks,
                  workersWithC2Tasks.size() >= 4);

        // Verify no worker is significantly more loaded than others
        int minTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).min().orElse(0);
        int maxTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).max().orElse(0);
        assertTrue("Task distribution should be relatively balanced. Min: " + minTasks + ", Max: " + maxTasks,
                  maxTasks - minTasks <= 2);
    }

    @Test
    public void testPerfectBalanceWith100TasksAnd3Workers() {
        // Test the exact scenario described: 100 tasks across 3 workers
        // Expected distribution: 33, 33, 34 (difference of at most 1)

        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker1").build(),
            new WorkerLoad.Builder("worker2").build(),
            new WorkerLoad.Builder("worker3").build()
        );

        // Create 100 tasks from different connectors
        List<ConnectorTaskId> tasks = new ArrayList<>();
        // 30 tasks for connector1
        for (int i = 0; i < 30; i++) {
            tasks.add(new ConnectorTaskId("connector1", i));
        }
        // 30 tasks for connector2
        for (int i = 0; i < 30; i++) {
            tasks.add(new ConnectorTaskId("connector2", i));
        }
        // 40 tasks for connector3
        for (int i = 0; i < 40; i++) {
            tasks.add(new ConnectorTaskId("connector3", i));
        }

        assertEquals("Should have exactly 100 tasks for the test", 100, tasks.size());

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks were assigned
        int totalAssignedTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals("All 100 tasks should be assigned", 100, totalAssignedTasks);

        // Get task counts for each worker
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);

        // CRITICAL ASSERTION: The difference between max and min should be at most 1
        assertTrue(String.format("Task distribution must have difference of at most 1. " +
                               "Current distribution: [%d, %d, %d], min=%d, max=%d, difference=%d",
                               taskCounts[0], taskCounts[1], taskCounts[2], 
                               minTasks, maxTasks, maxTasks - minTasks), maxTasks - minTasks <= 1);

        // For 100 tasks and 3 workers, expect exactly [33, 33, 34] in some order
        Arrays.sort(taskCounts);
        assertEquals("First worker should have 33 tasks", 33, taskCounts[0]);
        assertEquals("Second worker should have 33 tasks", 33, taskCounts[1]);
        assertEquals("Third worker should have 34 tasks", 34, taskCounts[2]);

        // Log distribution for verification
        for (WorkerLoad worker : workers) {
            System.out.println(worker.worker() + ": " + worker.tasksSize() + " tasks");
        }
    }

    @Test
    public void testPerfectBalanceWithVariousScenarios() {
        // Test multiple scenarios to ensure the "difference of 1" rule holds
        testBalanceScenario(7, 3);  // 7 tasks, 3 workers -> [2, 2, 3]
        testBalanceScenario(10, 4); // 10 tasks, 4 workers -> [2, 2, 3, 3]
        testBalanceScenario(15, 4); // 15 tasks, 4 workers -> [3, 4, 4, 4]
        testBalanceScenario(20, 6); // 20 tasks, 6 workers -> [3, 3, 3, 3, 4, 4]
        testBalanceScenario(50, 7); // 50 tasks, 7 workers -> [7, 7, 7, 7, 7, 7, 8]
    }

    private void testBalanceScenario(int taskCount, int workerCount) {
        // Create workers
        List<WorkerLoad> workers = new ArrayList<>();
        for (int i = 0; i < workerCount; i++) {
            workers.add(new WorkerLoad.Builder("worker" + i).build());
        }

        // Create tasks
        List<ConnectorTaskId> tasks = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new ConnectorTaskId("connector" + (i % 3), i));
        }

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks assigned
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(String.format("All %d tasks should be assigned", taskCount), taskCount, totalAssigned);

        // Verify balance constraint
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);

        assertTrue(String.format("FAILED: %d tasks, %d workers. Distribution: %s, " +
                               "min=%d, max=%d, difference=%d",
                               taskCount, workerCount, Arrays.toString(taskCounts),
                               minTasks, maxTasks, maxTasks - minTasks), maxTasks - minTasks <= 1);

        System.out.println(String.format("PASSED: %d tasks, %d workers -> %s", 
                                        taskCount, workerCount, Arrays.toString(taskCounts)));
    }

    @Test
    public void testMultipleWorkerFailuresWithSpotInstances() {
        // Create current assignment before spot instance termination
        List<WorkerLoad> survivingWorkers = Arrays.asList(
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 5), 
                             new ConnectorTaskId("C2", 0))
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 2), new ConnectorTaskId("C1", 7), 
                             new ConnectorTaskId("C2", 2))
            ).build(),
            new WorkerLoad.Builder("W5").withCopies(
                Arrays.asList("C1", "C2"), 
                Arrays.asList(new ConnectorTaskId("C1", 4), new ConnectorTaskId("C2", 4))
            ).build(),
            // New workers launched by autoscaling group (empty initially)
            new WorkerLoad.Builder("W6").withCopies(
                Collections.emptyList(), Collections.emptyList()
            ).build(),
            new WorkerLoad.Builder("W7").withCopies(
                Collections.emptyList(), Collections.emptyList()
            ).build()
        );

        // Tasks that need to be reassigned (from failed W2 and W9)
        List<ConnectorTaskId> tasksToReassign = Arrays.asList(
            // From W2: C1 C1 C2 C2 C1
            new ConnectorTaskId("C1", 10), new ConnectorTaskId("C1", 11),
            new ConnectorTaskId("C2", 10), new ConnectorTaskId("C2", 11),
            new ConnectorTaskId("C1", 12),
            // From W9: C1 C2 C5 C9
            new ConnectorTaskId("C1", 13), new ConnectorTaskId("C2", 12),
            new ConnectorTaskId("C5", 10), new ConnectorTaskId("C9", 10)
        );

        // Get initial task counts per worker
        int[] initialCounts = survivingWorkers.stream().mapToInt(WorkerLoad::tasksSize).toArray();

        // Perform global balance assignment
        assignor.assignTasks(survivingWorkers, tasksToReassign);

        // Verify all tasks were assigned
        int totalTasksAfter = survivingWorkers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        int totalTasksBefore = Arrays.stream(initialCounts).sum();
        assertEquals("All new tasks should be assigned", totalTasksBefore + tasksToReassign.size(), totalTasksAfter);

        // Verify balanced distribution across all 5 workers (including new ones)
        List<Integer> loads = survivingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());

        int minLoad = loads.get(0);
        int maxLoad = loads.get(loads.size() - 1);
        assertTrue("Load should be balanced with max difference of 1. Loads: " + loads, maxLoad - minLoad <= 1);

        // Verify global distribution - both connectors should be spread across workers
        long workersWithC1 = survivingWorkers.stream()
            .filter(w -> w.taskCountForConnector("C1") > 0)
            .count();
        long workersWithC2 = survivingWorkers.stream()
            .filter(w -> w.taskCountForConnector("C2") > 0)
            .count();

        assertTrue("C1 tasks should be spread across at least 4 workers, found: " + workersWithC1, workersWithC1 >= 4);
        assertTrue("C2 tasks should be spread across at least 4 workers, found: " + workersWithC2, workersWithC2 >= 4);

        // Verify that existing workers kept some of their original tasks (minimal movement)
        WorkerLoad w1 = survivingWorkers.stream().filter(w -> w.worker().equals("W1")).findFirst().orElse(null);
        WorkerLoad w3 = survivingWorkers.stream().filter(w -> w.worker().equals("W3")).findFirst().orElse(null);
        WorkerLoad w5 = survivingWorkers.stream().filter(w -> w.worker().equals("W5")).findFirst().orElse(null);

        if (w1 != null) {
            assertTrue("W1 should keep at least some C1 tasks", w1.taskCountForConnector("C1") >= 1);
        }
        if (w3 != null) {
            assertTrue("W3 should keep at least some C1 tasks", w3.taskCountForConnector("C1") >= 1);
        }
        if (w5 != null) {
            assertTrue("W5 should keep at least some C1 tasks", w5.taskCountForConnector("C1") >= 1);
        }

        // Verify new workers got tasks
        WorkerLoad w6 = survivingWorkers.stream().filter(w -> w.worker().equals("W6")).findFirst().orElse(null);
        WorkerLoad w7 = survivingWorkers.stream().filter(w -> w.worker().equals("W7")).findFirst().orElse(null);

        if (w6 != null) {
            assertTrue("New worker W6 should get balanced task load", w6.tasksSize() >= 2);
        }
        if (w7 != null) {
            assertTrue("New worker W7 should get balanced task load", w7.tasksSize() >= 2);
        }
    }

    @Test
    public void testWorkerScaleDown() {
        // Simulate autoscaling group scaling down from 5 to 3 workers
        // Workers W4 and W5 are terminated due to reduced load/cost optimization

        List<WorkerLoad> remainingWorkers = Arrays.asList(
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 5),
                             new ConnectorTaskId("C2", 0))
            ).build(),
            new WorkerLoad.Builder("W2").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 6),
                             new ConnectorTaskId("C2", 1))
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 2), new ConnectorTaskId("C1", 7),
                             new ConnectorTaskId("C2", 2))
            ).build()
        );

        // All tasks need to be redistributed among 3 workers
        List<ConnectorTaskId> allTasks = Arrays.asList(
            // C1 tasks (8 total)
            new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 2),
            new ConnectorTaskId("C1", 3), new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
            new ConnectorTaskId("C1", 6), new ConnectorTaskId("C1", 7),
            // C2 tasks (5 total)
            new ConnectorTaskId("C2", 0), new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
            new ConnectorTaskId("C2", 3), new ConnectorTaskId("C2", 4)
        );

        assignor.assignTasks(remainingWorkers, allTasks);

        // Verify all tasks assigned
        int totalTasks = remainingWorkers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals("All tasks should be assigned", 13, totalTasks);

        // Verify balanced distribution: 13 tasks / 3 workers = 4-4-5 or 4-5-4 distribution
        List<Integer> loads = remainingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());

        assertEquals(3, loads.size());
        assertTrue("Minimum load should be at least 4", loads.get(0) >= 4);
        assertTrue("Maximum load should be at most 5", loads.get(2) <= 5);
    }

    @Test
    public void testWorkerScaleUp() {
        // Simulate autoscaling group scaling up from 3 to 6 workers due to increased load

        List<WorkerLoad> expandedWorkers = Arrays.asList(
            // Existing workers with current load
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 3),
                             new ConnectorTaskId("C1", 6), new ConnectorTaskId("C2", 0),
                             new ConnectorTaskId("C2", 3))
            ).build(),
            new WorkerLoad.Builder("W2").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 4),
                             new ConnectorTaskId("C1", 7), new ConnectorTaskId("C2", 1),
                             new ConnectorTaskId("C2", 4))
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2"),
                Arrays.asList(new ConnectorTaskId("C1", 2), new ConnectorTaskId("C1", 5),
                             new ConnectorTaskId("C2", 2))
            ).build(),
            // New workers added by autoscaling group
            new WorkerLoad.Builder("W4").withCopies(Collections.emptyList(), Collections.emptyList()).build(),
            new WorkerLoad.Builder("W5").withCopies(Collections.emptyList(), Collections.emptyList()).build(),
            new WorkerLoad.Builder("W6").withCopies(Collections.emptyList(), Collections.emptyList()).build()
        );

        List<ConnectorTaskId> allTasks = Arrays.asList(
            // C1 tasks (8 total)
            new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 2),
            new ConnectorTaskId("C1", 3), new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
            new ConnectorTaskId("C1", 6), new ConnectorTaskId("C1", 7),
            // C2 tasks (5 total)
            new ConnectorTaskId("C2", 0), new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
            new ConnectorTaskId("C2", 3), new ConnectorTaskId("C2", 4)
        );

        assignor.assignTasks(expandedWorkers, allTasks);

        // Verify balanced distribution: 13 tasks / 6 workers = 2-2-2-2-2-3 distribution
        List<Integer> loads = expandedWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());

        int minLoad = loads.get(0);
        int maxLoad = loads.get(loads.size() - 1);
        assertTrue("Load should be balanced. Loads: " + loads, maxLoad - minLoad <= 1);

        // Verify tasks moved to new workers for better balance
        int newWorkerTasks = expandedWorkers.stream()
            .filter(w -> w.worker().equals("W4") || w.worker().equals("W5") || w.worker().equals("W6"))
            .mapToInt(WorkerLoad::tasksSize)
            .sum();

        assertTrue("New workers should get significant task load for balance", newWorkerTasks >= 6);
    }

    @Test
    public void testWorkerRemovalAndReaddition() {
        // Simulate complex scenario: Worker removal (W2 RIP) followed by re-addition
        // This tests the assignor's ability to handle dynamic worker topology changes
        // with minimal task disruption and optimal rebalancing

        // Phase 1: Setup initial state and verify baseline
        List<WorkerLoad> initialWorkers = createInitialWorkerState();
        verifyInitialState(initialWorkers);

        // Phase 2: Simulate W2 failure and verify redistribution
        List<WorkerLoad> survivingWorkers = createSurvivingWorkersAfterW2Failure();
        List<ConnectorTaskId> allTasks = createAllTasksList();

        // Perform rebalancing after W2 failure
        assignor.assignTasks(survivingWorkers, allTasks);
        verifyDistributionAfterW2Failure(survivingWorkers);

        // Phase 3: Simulate W2 return and verify optimal rebalancing
        List<WorkerLoad> workersWithW2Back = createWorkersWithW2Back(survivingWorkers);
        assignor.assignTasks(workersWithW2Back, allTasks);
        verifyFinalDistributionWithW2Back(workersWithW2Back);
    }

    private List<WorkerLoad> createInitialWorkerState() {
        return Arrays.asList(
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C5"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1),
                    new ConnectorTaskId("C1", 2), new ConnectorTaskId("C1", 3),
                    new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
                    new ConnectorTaskId("C5", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W2").withCopies(
                Arrays.asList("C1", "C2", "C3", "C6"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 6), new ConnectorTaskId("C1", 7),
                    new ConnectorTaskId("C1", 8), new ConnectorTaskId("C1", 9),
                    new ConnectorTaskId("C2", 0), new ConnectorTaskId("C3", 0),
                    new ConnectorTaskId("C6", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2", "C3", "C7"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 10), new ConnectorTaskId("C3", 1),
                    new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
                    new ConnectorTaskId("C2", 3), new ConnectorTaskId("C3", 2),
                    new ConnectorTaskId("C7", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W4").withCopies(
                Arrays.asList("C1", "C2", "C4", "C8"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 11), new ConnectorTaskId("C1", 12),
                    new ConnectorTaskId("C1", 13), new ConnectorTaskId("C2", 4),
                    new ConnectorTaskId("C2", 5), new ConnectorTaskId("C4", 0),
                    new ConnectorTaskId("C8", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W5").withCopies(
                Arrays.asList("C1", "C2", "C4", "C8"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 14), new ConnectorTaskId("C1", 15),
                    new ConnectorTaskId("C1", 16), new ConnectorTaskId("C2", 6),
                    new ConnectorTaskId("C2", 7), new ConnectorTaskId("C4", 1),
                    new ConnectorTaskId("C8", 1)
                )
            ).build()
        );
    }

    private void verifyInitialState(List<WorkerLoad> initialWorkers) {
        assertEquals("W1 should have 7 tasks initially", 7, initialWorkers.get(0).tasksSize());
        assertEquals("W2 should have 7 tasks initially", 7, initialWorkers.get(1).tasksSize());
        assertEquals("W3 should have 7 tasks initially", 7, initialWorkers.get(2).tasksSize());
        assertEquals("W4 should have 7 tasks initially", 7, initialWorkers.get(3).tasksSize());
        assertEquals("W5 should have 7 tasks initially", 7, initialWorkers.get(4).tasksSize());
    }

    private List<WorkerLoad> createSurvivingWorkersAfterW2Failure() {
        return Arrays.asList(
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1", "C5"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1),
                    new ConnectorTaskId("C1", 2), new ConnectorTaskId("C1", 3),
                    new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
                    new ConnectorTaskId("C5", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList("C1", "C2", "C3", "C7"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 10), new ConnectorTaskId("C3", 1),
                    new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
                    new ConnectorTaskId("C2", 3), new ConnectorTaskId("C3", 2),
                    new ConnectorTaskId("C7", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W4").withCopies(
                Arrays.asList("C1", "C2", "C4", "C8"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 11), new ConnectorTaskId("C1", 12),
                    new ConnectorTaskId("C1", 13), new ConnectorTaskId("C2", 4),
                    new ConnectorTaskId("C2", 5), new ConnectorTaskId("C4", 0),
                    new ConnectorTaskId("C8", 0)
                )
            ).build(),
            new WorkerLoad.Builder("W5").withCopies(
                Arrays.asList("C1", "C2", "C4", "C8"),
                Arrays.asList(
                    new ConnectorTaskId("C1", 14), new ConnectorTaskId("C1", 15),
                    new ConnectorTaskId("C1", 16), new ConnectorTaskId("C2", 6),
                    new ConnectorTaskId("C2", 7), new ConnectorTaskId("C4", 1),
                    new ConnectorTaskId("C8", 1)
                )
            ).build()
        );
    }

    private List<ConnectorTaskId> createAllTasksList() {
        List<ConnectorTaskId> tasks = new ArrayList<>();
        // C1 tasks (17 total: 0-16)
        for (int i = 0; i < 17; i++) {
            tasks.add(new ConnectorTaskId("C1", i));
        }
        // C2 tasks (8 total: 0-7)
        for (int i = 0; i < 8; i++) {
            tasks.add(new ConnectorTaskId("C2", i));
        }
        // C3 tasks (3 total: 0-2)
        for (int i = 0; i < 3; i++) {
            tasks.add(new ConnectorTaskId("C3", i));
        }
        // Other connectors
        tasks.add(new ConnectorTaskId("C4", 0));
        tasks.add(new ConnectorTaskId("C4", 1));
        tasks.add(new ConnectorTaskId("C5", 0));
        tasks.add(new ConnectorTaskId("C6", 0));
        tasks.add(new ConnectorTaskId("C7", 0));
        tasks.add(new ConnectorTaskId("C8", 0));
        tasks.add(new ConnectorTaskId("C8", 1));
        return tasks;
    }

    private void verifyDistributionAfterW2Failure(List<WorkerLoad> survivingWorkers) {
        int totalTasksAfterFailure = survivingWorkers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals("All 35 tasks should be redistributed among 4 workers", 35, totalTasksAfterFailure);

        List<Integer> loadsAfterFailure = survivingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());

        int minLoadAfterFailure = loadsAfterFailure.get(0);
        int maxLoadAfterFailure = loadsAfterFailure.get(loadsAfterFailure.size() - 1);
        assertTrue("Load should be balanced after W2 failure. Loads: " + loadsAfterFailure,
                  maxLoadAfterFailure - minLoadAfterFailure <= 1);
    }

    private List<WorkerLoad> createWorkersWithW2Back(List<WorkerLoad> survivingWorkers) {
        return Arrays.asList(
            survivingWorkers.get(0), // W1
            new WorkerLoad.Builder("W2").withCopies(Collections.emptyList(), Collections.emptyList()).build(),
            survivingWorkers.get(1), // W3
            survivingWorkers.get(2), // W4
            survivingWorkers.get(3)  // W5
        );
    }

    private void verifyFinalDistributionWithW2Back(List<WorkerLoad> workersWithW2Back) {
        int totalTasksWithW2Back = workersWithW2Back.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals("All 35 tasks should be assigned with W2 back", 35, totalTasksWithW2Back);

        List<Integer> loadsWithW2Back = workersWithW2Back.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());

        int minLoadWithW2Back = loadsWithW2Back.get(0);
        int maxLoadWithW2Back = loadsWithW2Back.get(loadsWithW2Back.size() - 1);
        assertTrue("Load should be perfectly balanced with W2 back. Loads: " + loadsWithW2Back,
                  maxLoadWithW2Back - minLoadWithW2Back <= 1);

        WorkerLoad w2Back = workersWithW2Back.stream()
            .filter(w -> w.worker().equals("W2"))
            .findFirst().orElse(null);
        if (w2Back != null) {
            assertTrue("W2 should get significant load when it returns", w2Back.tasksSize() >= 6);
        }

        for (String connector : Arrays.asList("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8")) {
            long workersWithConnector = workersWithW2Back.stream()
                .filter(w -> w.taskCountForConnector(connector) > 0)
                .count();
            if (connector.equals("C1") || connector.equals("C2")) {
                assertTrue("Major connector " + connector + " should be spread across at least 4 workers",
                          workersWithConnector >= 4);
            }
        }
    }

    /**
     * Test class for GlobalBalanceAssignor functionality
     * Tests the core requirement: maximum difference of 1 task between workers
     */
    public static void main(String[] args) {
        GlobalBalanceAssignorTest test = new GlobalBalanceAssignorTest();
        
        System.out.println("Running GlobalBalanceAssignor tests...");
        
        try {
            // Test 1: Basic Task Assignment
            test.setup();
            test.testBasicTaskAssignment();
            System.out.println("✓ testBasicTaskAssignment passed");
            
            // Test 2: Multiple Connector Task Assignment
            test.setup();
            test.testMultipleConnectorTaskAssignment();
            System.out.println("✓ testMultipleConnectorTaskAssignment passed");
            
            // Test 3: Connector Assignment
            test.setup();
            test.testConnectorAssignment();
            System.out.println("✓ testConnectorAssignment passed");
            
            // Test 4: Perfect Task Balance Requirement (Critical Test)
            test.setup();
            test.testPerfectTaskBalanceRequirement();
            System.out.println("✓ testPerfectTaskBalanceRequirement passed");
            
            // Test 5: Original Scenario Failure and Reassignment
            test.setup();
            test.testOriginalScenarioFailureAndReassignment();
            System.out.println("✓ testOriginalScenarioFailureAndReassignment passed");
            
            // Test 6: Perfect Balance with 100 Tasks and 3 Workers (Main Example)
            test.setup();
            test.testPerfectBalanceWith100TasksAnd3Workers();
            System.out.println("✓ testPerfectBalanceWith100TasksAnd3Workers passed");
            
            // Test 7: Perfect Balance with Various Scenarios
            test.setup();
            test.testPerfectBalanceWithVariousScenarios();
            System.out.println("✓ testPerfectBalanceWithVariousScenarios passed");
            
            // Test 8: Multiple Worker Failures with Spot Instances
            test.setup();
            test.testMultipleWorkerFailuresWithSpotInstances();
            System.out.println("✓ testMultipleWorkerFailuresWithSpotInstances passed");
            
            // Test 9: Worker Scale Down
            test.setup();
            test.testWorkerScaleDown();
            System.out.println("✓ testWorkerScaleDown passed");
            
            // Test 10: Worker Scale Up
            test.setup();
            test.testWorkerScaleUp();
            System.out.println("✓ testWorkerScaleUp passed");
            
            // Test 11: Worker Removal and Re-addition
            test.setup();
            test.testWorkerRemovalAndReaddition();
            System.out.println("✓ testWorkerRemovalAndReaddition passed");
            
            System.out.println("\n🎉 All GlobalBalanceAssignor tests passed!");
            System.out.println("✅ Perfect load balancing verified: max difference of 1 task between workers");
            System.out.println("✅ Global task distribution confirmed: tasks spread across workers");
            System.out.println("✅ Autoscaling scenarios tested successfully");
            
        } catch (AssertionError e) {
            System.err.println("\n❌ Test failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } catch (Exception e) {
            System.err.println("\n💥 Test error: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}

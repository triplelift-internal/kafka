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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validation test for the exact scenario described in the requirements.
 */
public class GlobalBalanceAssignorScenarioTest {

    private GlobalBalanceAssignor assignor;
    private MockTime time;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
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
        assertEquals(totalTasksBefore + tasksToReassign.size(), totalTasksAfter, 
                    "All new tasks should be assigned");

        // Verify global balance: tasks for same connector are spread across workers
        
        // Check C1 task distribution (should be spread across multiple workers)
        List<String> workersWithC1Tasks = workers.stream()
            .filter(w -> w.taskCountForConnector("C1") > 0)
            .map(WorkerLoad::worker)
            .collect(Collectors.toList());
        assertTrue(workersWithC1Tasks.size() >= 4, 
                  "C1 tasks should be spread across at least 4 workers, but found in: " + workersWithC1Tasks);

        // Check C2 task distribution (should be spread across multiple workers)
        List<String> workersWithC2Tasks = workers.stream()
            .filter(w -> w.taskCountForConnector("C2") > 0)
            .map(WorkerLoad::worker)
            .collect(Collectors.toList());
        assertTrue(workersWithC2Tasks.size() >= 4, 
                  "C2 tasks should be spread across at least 4 workers, but found in: " + workersWithC2Tasks);

        // Verify no worker is significantly more loaded than others
        int minTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).min().orElse(0);
        int maxTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).max().orElse(0);
        assertTrue(maxTasks - minTasks <= 2, 
                  "Task distribution should be relatively balanced. Min: " + minTasks + ", Max: " + maxTasks);

        // Log final distribution for verification
        for (WorkerLoad worker : workers) {
            System.out.println(worker.worker() + ": " + worker.tasksSize() + " tasks, " +
                             "C1 tasks: " + worker.taskCountForConnector("C1") + ", " +
                             "C2 tasks: " + worker.taskCountForConnector("C2"));
        }
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
        List<ConnectorTaskId> tasks = Arrays.asList(
            // 30 tasks for connector1
            new ConnectorTaskId("connector1", 0), new ConnectorTaskId("connector1", 1),
            new ConnectorTaskId("connector1", 2), new ConnectorTaskId("connector1", 3),
            new ConnectorTaskId("connector1", 4), new ConnectorTaskId("connector1", 5),
            new ConnectorTaskId("connector1", 6), new ConnectorTaskId("connector1", 7),
            new ConnectorTaskId("connector1", 8), new ConnectorTaskId("connector1", 9),
            new ConnectorTaskId("connector1", 10), new ConnectorTaskId("connector1", 11),
            new ConnectorTaskId("connector1", 12), new ConnectorTaskId("connector1", 13),
            new ConnectorTaskId("connector1", 14), new ConnectorTaskId("connector1", 15),
            new ConnectorTaskId("connector1", 16), new ConnectorTaskId("connector1", 17),
            new ConnectorTaskId("connector1", 18), new ConnectorTaskId("connector1", 19),
            new ConnectorTaskId("connector1", 20), new ConnectorTaskId("connector1", 21),
            new ConnectorTaskId("connector1", 22), new ConnectorTaskId("connector1", 23),
            new ConnectorTaskId("connector1", 24), new ConnectorTaskId("connector1", 25),
            new ConnectorTaskId("connector1", 26), new ConnectorTaskId("connector1", 27),
            new ConnectorTaskId("connector1", 28), new ConnectorTaskId("connector1", 29),
            
            // 30 tasks for connector2
            new ConnectorTaskId("connector2", 0), new ConnectorTaskId("connector2", 1),
            new ConnectorTaskId("connector2", 2), new ConnectorTaskId("connector2", 3),
            new ConnectorTaskId("connector2", 4), new ConnectorTaskId("connector2", 5),
            new ConnectorTaskId("connector2", 6), new ConnectorTaskId("connector2", 7),
            new ConnectorTaskId("connector2", 8), new ConnectorTaskId("connector2", 9),
            new ConnectorTaskId("connector2", 10), new ConnectorTaskId("connector2", 11),
            new ConnectorTaskId("connector2", 12), new ConnectorTaskId("connector2", 13),
            new ConnectorTaskId("connector2", 14), new ConnectorTaskId("connector2", 15),
            new ConnectorTaskId("connector2", 16), new ConnectorTaskId("connector2", 17),
            new ConnectorTaskId("connector2", 18), new ConnectorTaskId("connector2", 19),
            new ConnectorTaskId("connector2", 20), new ConnectorTaskId("connector2", 21),
            new ConnectorTaskId("connector2", 22), new ConnectorTaskId("connector2", 23),
            new ConnectorTaskId("connector2", 24), new ConnectorTaskId("connector2", 25),
            new ConnectorTaskId("connector2", 26), new ConnectorTaskId("connector2", 27),
            new ConnectorTaskId("connector2", 28), new ConnectorTaskId("connector2", 29),
            
            // 40 tasks for connector3  
            new ConnectorTaskId("connector3", 0), new ConnectorTaskId("connector3", 1),
            new ConnectorTaskId("connector3", 2), new ConnectorTaskId("connector3", 3),
            new ConnectorTaskId("connector3", 4), new ConnectorTaskId("connector3", 5),
            new ConnectorTaskId("connector3", 6), new ConnectorTaskId("connector3", 7),
            new ConnectorTaskId("connector3", 8), new ConnectorTaskId("connector3", 9),
            new ConnectorTaskId("connector3", 10), new ConnectorTaskId("connector3", 11),
            new ConnectorTaskId("connector3", 12), new ConnectorTaskId("connector3", 13),
            new ConnectorTaskId("connector3", 14), new ConnectorTaskId("connector3", 15),
            new ConnectorTaskId("connector3", 16), new ConnectorTaskId("connector3", 17),
            new ConnectorTaskId("connector3", 18), new ConnectorTaskId("connector3", 19),
            new ConnectorTaskId("connector3", 20), new ConnectorTaskId("connector3", 21),
            new ConnectorTaskId("connector3", 22), new ConnectorTaskId("connector3", 23),
            new ConnectorTaskId("connector3", 24), new ConnectorTaskId("connector3", 25),
            new ConnectorTaskId("connector3", 26), new ConnectorTaskId("connector3", 27),
            new ConnectorTaskId("connector3", 28), new ConnectorTaskId("connector3", 29),
            new ConnectorTaskId("connector3", 30), new ConnectorTaskId("connector3", 31),
            new ConnectorTaskId("connector3", 32), new ConnectorTaskId("connector3", 33),
            new ConnectorTaskId("connector3", 34), new ConnectorTaskId("connector3", 35),
            new ConnectorTaskId("connector3", 36), new ConnectorTaskId("connector3", 37),
            new ConnectorTaskId("connector3", 38), new ConnectorTaskId("connector3", 39)
        );

        assertEquals(100, tasks.size(), "Should have exactly 100 tasks for the test");

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks were assigned
        int totalAssignedTasks = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(100, totalAssignedTasks, "All 100 tasks should be assigned");

        // Get task counts for each worker
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);

        // CRITICAL ASSERTION: The difference between max and min should be at most 1
        assertTrue(maxTasks - minTasks <= 1, 
                  String.format("Task distribution must have difference of at most 1. " +
                               "Current distribution: [%d, %d, %d], min=%d, max=%d, difference=%d",
                               taskCounts[0], taskCounts[1], taskCounts[2], 
                               minTasks, maxTasks, maxTasks - minTasks));

        // For 100 tasks and 3 workers, expect exactly [33, 33, 34] in some order
        Arrays.sort(taskCounts);
        assertEquals(33, taskCounts[0], "First worker should have 33 tasks");
        assertEquals(33, taskCounts[1], "Second worker should have 33 tasks");
        assertEquals(34, taskCounts[2], "Third worker should have 34 tasks");

        // Log distribution for verification
        for (int i = 0; i < workers.size(); i++) {
            WorkerLoad worker = workers.get(i);
            System.out.println(String.format("%s: %d tasks", worker.worker(), worker.tasksSize()));
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
        List<WorkerLoad> workers = java.util.stream.IntStream.range(0, workerCount)
                .mapToObj(i -> new WorkerLoad.Builder("worker" + i).build())
                .collect(Collectors.toList());

        // Create tasks
        List<ConnectorTaskId> tasks = java.util.stream.IntStream.range(0, taskCount)
                .mapToObj(i -> new ConnectorTaskId("connector" + (i % 3), i))
                .collect(Collectors.toList());

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks assigned
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(taskCount, totalAssigned, 
                    String.format("All %d tasks should be assigned", taskCount));

        // Verify balance constraint
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);

        assertTrue(maxTasks - minTasks <= 1, 
                  String.format("FAILED: %d tasks, %d workers. Distribution: %s, " +
                               "min=%d, max=%d, difference=%d",
                               taskCount, workerCount, Arrays.toString(taskCounts),
                               minTasks, maxTasks, maxTasks - minTasks));

        System.out.println(String.format("PASSED: %d tasks, %d workers -> %s", 
                                        taskCount, workerCount, Arrays.toString(taskCounts)));
    }
}
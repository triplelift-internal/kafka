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
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify perfect task balance with maximum difference of 1 between workers.
 */
public class GlobalBalanceAssignorPerfectBalanceTest {

    private GlobalBalanceAssignor assignor;
    private MockTime time;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
    }

    @Test
    public void testPerfectBalanceWith100TasksAnd3Workers() {
        // Test the exact scenario: 100 tasks across 3 workers
        // Expected: [33, 33, 34] (difference of at most 1)
        verifyPerfectBalance(100, 3);
    }

    @Test
    public void testPerfectBalanceMultipleScenarios() {
        // Test various scenarios to ensure perfect balance
        verifyPerfectBalance(7, 3);   // [2, 2, 3]
        verifyPerfectBalance(10, 4);  // [2, 2, 3, 3] 
        verifyPerfectBalance(15, 4);  // [3, 4, 4, 4]
        verifyPerfectBalance(20, 6);  // [3, 3, 3, 3, 4, 4]
        verifyPerfectBalance(50, 7);  // [7, 7, 7, 7, 7, 7, 8]
        verifyPerfectBalance(1, 5);   // [0, 0, 0, 0, 1]
        verifyPerfectBalance(99, 10); // [9, 9, 10, 10, 10, 10, 10, 10, 10, 10]
    }

    private void verifyPerfectBalance(int taskCount, int workerCount) {
        System.out.println(String.format("\n=== Testing %d tasks with %d workers ===", taskCount, workerCount));
        
        // Create workers
        List<WorkerLoad> workers = IntStream.range(0, workerCount)
                .mapToObj(i -> new WorkerLoad.Builder("worker" + i).build())
                .collect(Collectors.toList());

        // Create tasks from multiple connectors for realistic distribution
        List<ConnectorTaskId> tasks = IntStream.range(0, taskCount)
                .mapToObj(i -> new ConnectorTaskId("connector" + (i % 5), i))
                .collect(Collectors.toList());

        // Perform assignment
        assignor.assignTasks(workers, tasks);

        // Verify all tasks assigned
        int totalAssigned = workers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(taskCount, totalAssigned, 
                    String.format("All %d tasks should be assigned", taskCount));

        // Get task distribution
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);
        int difference = maxTasks - minTasks;

        // Log the distribution
        System.out.println("Actual distribution: " + Arrays.toString(taskCounts));
        System.out.println(String.format("Min: %d, Max: %d, Difference: %d", minTasks, maxTasks, difference));

        // Calculate expected perfect distribution
        int baseTasksPerWorker = taskCount / workerCount;
        int workersWithExtraTask = taskCount % workerCount;
        
        System.out.println(String.format("Expected: %d workers with %d tasks, %d workers with %d tasks", 
                                        workerCount - workersWithExtraTask, baseTasksPerWorker,
                                        workersWithExtraTask, baseTasksPerWorker + 1));

        // CRITICAL ASSERTION: The difference between max and min should be at most 1
        assertTrue(difference <= 1, 
                  String.format("FAILED: Task distribution difference must be ≤ 1. " +
                               "Distribution: %s, min=%d, max=%d, difference=%d",
                               Arrays.toString(taskCounts), minTasks, maxTasks, difference));

        // Verify exact distribution matches expected perfect balance
        Arrays.sort(taskCounts);
        for (int i = 0; i < workerCount; i++) {
            int expectedTasks = baseTasksPerWorker + (i >= (workerCount - workersWithExtraTask) ? 1 : 0);
            assertEquals(expectedTasks, taskCounts[i], 
                        String.format("Worker %d should have exactly %d tasks, but has %d", 
                                     i, expectedTasks, taskCounts[i]));
        }

        System.out.println("✓ PERFECT BALANCE ACHIEVED");
    }

    @Test 
    public void testIncrementalAssignmentMaintainsBalance() {
        System.out.println("\n=== Testing incremental assignment balance ===");
        
        // Start with 3 workers and some existing tasks
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker1").withCopies(
                Arrays.asList("connector1"), 
                Arrays.asList(new ConnectorTaskId("connector1", 0), new ConnectorTaskId("connector1", 1))
            ).build(),
            new WorkerLoad.Builder("worker2").withCopies(
                Arrays.asList("connector1"), 
                Arrays.asList(new ConnectorTaskId("connector1", 2), new ConnectorTaskId("connector1", 3))
            ).build(),
            new WorkerLoad.Builder("worker3").withCopies(
                Arrays.asList("connector1"), 
                Arrays.asList(new ConnectorTaskId("connector1", 4))
            ).build()
        );

        // Log initial state
        System.out.println("Initial distribution: " + Arrays.toString(
            workers.stream().mapToInt(WorkerLoad::tasksSize).toArray()));

        // Add 7 new tasks to be assigned incrementally
        List<ConnectorTaskId> newTasks = Arrays.asList(
            new ConnectorTaskId("connector2", 0), new ConnectorTaskId("connector2", 1),
            new ConnectorTaskId("connector2", 2), new ConnectorTaskId("connector2", 3),
            new ConnectorTaskId("connector2", 4), new ConnectorTaskId("connector2", 5),
            new ConnectorTaskId("connector2", 6)
        );

        // Perform incremental assignment
        assignor.assignTasks(workers, newTasks);

        // Verify balance after incremental assignment
        int[] finalCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(finalCounts).min().orElse(0);
        int maxTasks = Arrays.stream(finalCounts).max().orElse(0);
        int difference = maxTasks - minTasks;

        System.out.println("Final distribution: " + Arrays.toString(finalCounts));
        System.out.println(String.format("Min: %d, Max: %d, Difference: %d", minTasks, maxTasks, difference));

        // After incremental assignment, balance should still be maintained
        assertTrue(difference <= 1, 
                  String.format("After incremental assignment, difference should be ≤ 1. " +
                               "Distribution: %s, difference=%d", 
                               Arrays.toString(finalCounts), difference));

        System.out.println("✓ INCREMENTAL ASSIGNMENT MAINTAINS BALANCE");
    }
}
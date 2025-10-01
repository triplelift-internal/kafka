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
 * Test for cloud autoscaling scenarios with preemptible/spot instance terminations.
 */
public class GlobalBalanceAssignorAutoscalingTest {

    private GlobalBalanceAssignor assignor;
    private MockTime time;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
    }

    @Test
    public void testMultipleWorkerFailuresWithPreemptibleInstances() {
        // Simulate cloud autoscaling scenario: Multiple preemptible instances terminated simultaneously
        // Initial state: 5 workers with balanced distribution
        // Failures: W2 and W4 terminated by cloud provider interruption
        // New workers: W6 and W7 launched by autoscaling group
        
        // Create current assignment before preemptible instance termination
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
                Arrays.asList(), Arrays.asList()
            ).build(),
            new WorkerLoad.Builder("W7").withCopies(
                Arrays.asList(), Arrays.asList()
            ).build()
        );

        // All tasks that should be distributed (includes tasks from terminated workers)
        List<ConnectorTaskId> allTasks = Arrays.asList(
            // C1 tasks (8 total)
            new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 2), 
            new ConnectorTaskId("C1", 3), new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
            new ConnectorTaskId("C1", 6), new ConnectorTaskId("C1", 7),
            // C2 tasks (5 total)
            new ConnectorTaskId("C2", 0), new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
            new ConnectorTaskId("C2", 3), new ConnectorTaskId("C2", 4)
        );

        // Perform assignment with autoscaling-aware algorithm
        assignor.assignTasks(survivingWorkers, allTasks);

        // Verify balanced distribution across all 5 workers (including new ones)
        // Expected: 13 tasks across 5 workers = 2-3 tasks per worker (some get 3, some get 2)
        
        int totalTasks = survivingWorkers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(13, totalTasks, "All tasks should be assigned");

        // Check that load is balanced (max difference of 1 task)
        List<Integer> loads = survivingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());
        
        int minLoad = loads.get(0);
        int maxLoad = loads.get(loads.size() - 1);
        assertTrue(maxLoad - minLoad <= 1, 
                  "Load should be balanced with max difference of 1. Loads: " + loads);

        // Verify global distribution - both connectors should be spread across workers
        long workersWithC1 = survivingWorkers.stream()
            .filter(w -> w.taskCountForConnector("C1") > 0)
            .count();
        long workersWithC2 = survivingWorkers.stream()
            .filter(w -> w.taskCountForConnector("C2") > 0)
            .count();
        
        assertTrue(workersWithC1 >= 4, 
                  "C1 tasks should be spread across at least 4 workers, found: " + workersWithC1);
        assertTrue(workersWithC2 >= 4, 
                  "C2 tasks should be spread across at least 4 workers, found: " + workersWithC2);

        // Verify that existing workers kept some of their original tasks (minimal movement)
        WorkerLoad w1 = survivingWorkers.stream().filter(w -> w.worker().equals("W1")).findFirst().orElse(null);
        WorkerLoad w3 = survivingWorkers.stream().filter(w -> w.worker().equals("W3")).findFirst().orElse(null);
        WorkerLoad w5 = survivingWorkers.stream().filter(w -> w.worker().equals("W5")).findFirst().orElse(null);
        
        assertTrue(w1.taskCountForConnector("C1") >= 1, "W1 should keep at least some C1 tasks");
        assertTrue(w3.taskCountForConnector("C1") >= 1, "W3 should keep at least some C1 tasks");
        assertTrue(w5.taskCountForConnector("C1") >= 1, "W5 should keep at least some C1 tasks");

        // Verify new workers got tasks
        WorkerLoad w6 = survivingWorkers.stream().filter(w -> w.worker().equals("W6")).findFirst().orElse(null);
        WorkerLoad w7 = survivingWorkers.stream().filter(w -> w.worker().equals("W7")).findFirst().orElse(null);
        
        assertTrue(w6.tasksSize() >= 2, "New worker W6 should get balanced task load");
        assertTrue(w7.tasksSize() >= 2, "New worker W7 should get balanced task load");
        
        System.out.println("Final distribution after autoscaling:");
        for (WorkerLoad worker : survivingWorkers) {
            System.out.printf("Worker %s: %d total tasks (C1: %d, C2: %d)%n", 
                             worker.worker(), worker.tasksSize(),
                             worker.taskCountForConnector("C1"),
                             worker.taskCountForConnector("C2"));
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
        assertEquals(13, totalTasks, "All tasks should be assigned");

        // Verify balanced distribution: 13 tasks / 3 workers = 4-4-5 or 4-5-4 distribution
        List<Integer> loads = remainingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());
        
        assertEquals(3, loads.size());
        assertTrue(loads.get(0) >= 4, "Minimum load should be at least 4");
        assertTrue(loads.get(2) <= 5, "Maximum load should be at most 5");
        
        System.out.println("Final distribution after scale down:");
        for (WorkerLoad worker : remainingWorkers) {
            System.out.printf("Worker %s: %d total tasks (C1: %d, C2: %d)%n", 
                             worker.worker(), worker.tasksSize(),
                             worker.taskCountForConnector("C1"),
                             worker.taskCountForConnector("C2"));
        }
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
            new WorkerLoad.Builder("W4").withCopies(Arrays.asList(), Arrays.asList()).build(),
            new WorkerLoad.Builder("W5").withCopies(Arrays.asList(), Arrays.asList()).build(),
            new WorkerLoad.Builder("W6").withCopies(Arrays.asList(), Arrays.asList()).build()
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
        assertTrue(maxLoad - minLoad <= 1, 
                  "Load should be balanced. Loads: " + loads);
        
        // Verify tasks moved to new workers for better balance
        int newWorkerTasks = expandedWorkers.stream()
            .filter(w -> w.worker().equals("W4") || w.worker().equals("W5") || w.worker().equals("W6"))
            .mapToInt(WorkerLoad::tasksSize)
            .sum();
        
        assertTrue(newWorkerTasks >= 6, "New workers should get significant task load for balance");
        
        System.out.println("Final distribution after scale up:");
        for (WorkerLoad worker : expandedWorkers) {
            System.out.printf("Worker %s: %d total tasks (C1: %d, C2: %d)%n", 
                             worker.worker(), worker.tasksSize(),
                             worker.taskCountForConnector("C1"),
                             worker.taskCountForConnector("C2"));
        }
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
        assertEquals(7, initialWorkers.get(0).tasksSize(), "W1 should have 7 tasks initially");
        assertEquals(7, initialWorkers.get(1).tasksSize(), "W2 should have 7 tasks initially");
        assertEquals(7, initialWorkers.get(2).tasksSize(), "W3 should have 7 tasks initially");
        assertEquals(7, initialWorkers.get(3).tasksSize(), "W4 should have 7 tasks initially");
        assertEquals(7, initialWorkers.get(4).tasksSize(), "W5 should have 7 tasks initially");
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
        return Arrays.asList(
            // C1 tasks (17 total: 0-16)
            new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1), new ConnectorTaskId("C1", 2),
            new ConnectorTaskId("C1", 3), new ConnectorTaskId("C1", 4), new ConnectorTaskId("C1", 5),
            new ConnectorTaskId("C1", 6), new ConnectorTaskId("C1", 7), new ConnectorTaskId("C1", 8),
            new ConnectorTaskId("C1", 9), new ConnectorTaskId("C1", 10), new ConnectorTaskId("C1", 11),
            new ConnectorTaskId("C1", 12), new ConnectorTaskId("C1", 13), new ConnectorTaskId("C1", 14),
            new ConnectorTaskId("C1", 15), new ConnectorTaskId("C1", 16),
            // C2 tasks (8 total: 0-7)
            new ConnectorTaskId("C2", 0), new ConnectorTaskId("C2", 1), new ConnectorTaskId("C2", 2),
            new ConnectorTaskId("C2", 3), new ConnectorTaskId("C2", 4), new ConnectorTaskId("C2", 5),
            new ConnectorTaskId("C2", 6), new ConnectorTaskId("C2", 7),
            // C3 tasks (3 total: 0-2)
            new ConnectorTaskId("C3", 0), new ConnectorTaskId("C3", 1), new ConnectorTaskId("C3", 2),
            // Single task connectors
            new ConnectorTaskId("C4", 0), new ConnectorTaskId("C4", 1),
            new ConnectorTaskId("C5", 0), new ConnectorTaskId("C6", 0), new ConnectorTaskId("C7", 0),
            new ConnectorTaskId("C8", 0), new ConnectorTaskId("C8", 1)
        );
    }

    private void verifyDistributionAfterW2Failure(List<WorkerLoad> survivingWorkers) {
        int totalTasksAfterFailure = survivingWorkers.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(35, totalTasksAfterFailure, "All 35 tasks should be redistributed among 4 workers");

        List<Integer> loadsAfterFailure = survivingWorkers.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());
        
        int minLoadAfterFailure = loadsAfterFailure.get(0);
        int maxLoadAfterFailure = loadsAfterFailure.get(loadsAfterFailure.size() - 1);
        assertTrue(maxLoadAfterFailure - minLoadAfterFailure <= 1, 
                  "Load should be balanced after W2 failure. Loads: " + loadsAfterFailure);

        System.out.println("Distribution after W2 failure (4 workers):");
        for (WorkerLoad worker : survivingWorkers) {
            System.out.printf("Worker %s: %d total tasks%n", worker.worker(), worker.tasksSize());
        }
    }

    private List<WorkerLoad> createWorkersWithW2Back(List<WorkerLoad> survivingWorkers) {
        return Arrays.asList(
            survivingWorkers.get(0), // W1
            new WorkerLoad.Builder("W2").withCopies(Arrays.asList(), Arrays.asList()).build(),
            survivingWorkers.get(1), // W3
            survivingWorkers.get(2), // W4
            survivingWorkers.get(3)  // W5
        );
    }

    private void verifyFinalDistributionWithW2Back(List<WorkerLoad> workersWithW2Back) {
        int totalTasksWithW2Back = workersWithW2Back.stream().mapToInt(WorkerLoad::tasksSize).sum();
        assertEquals(35, totalTasksWithW2Back, "All 35 tasks should be assigned with W2 back");

        List<Integer> loadsWithW2Back = workersWithW2Back.stream()
            .map(WorkerLoad::tasksSize)
            .sorted()
            .collect(Collectors.toList());
        
        int minLoadWithW2Back = loadsWithW2Back.get(0);
        int maxLoadWithW2Back = loadsWithW2Back.get(loadsWithW2Back.size() - 1);
        assertTrue(maxLoadWithW2Back - minLoadWithW2Back <= 1, 
                  "Load should be perfectly balanced with W2 back. Loads: " + loadsWithW2Back);

        WorkerLoad w2Back = workersWithW2Back.stream()
            .filter(w -> w.worker().equals("W2"))
            .findFirst().orElse(null);
        assertTrue(w2Back.tasksSize() >= 6, "W2 should get significant load when it returns");

        for (String connector : Arrays.asList("C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8")) {
            long workersWithConnector = workersWithW2Back.stream()
                .filter(w -> w.taskCountForConnector(connector) > 0)
                .count();
            if (connector.equals("C1") || connector.equals("C2")) {
                assertTrue(workersWithConnector >= 4, 
                          "Major connector " + connector + " should be spread across at least 4 workers");
            }
        }

        System.out.println("Final distribution with W2 back online (5 workers):");
        for (WorkerLoad worker : workersWithW2Back) {
            System.out.printf("Worker %s: %d total tasks%n", worker.worker(), worker.tasksSize());
        }

        // Verify minimal task movement by checking that existing workers maintain reasonable load
        WorkerLoad w1Final = workersWithW2Back.stream().filter(w -> w.worker().equals("W1")).findFirst().orElse(null);
        WorkerLoad w3Final = workersWithW2Back.stream().filter(w -> w.worker().equals("W3")).findFirst().orElse(null);
        WorkerLoad w4Final = workersWithW2Back.stream().filter(w -> w.worker().equals("W4")).findFirst().orElse(null);
        WorkerLoad w5Final = workersWithW2Back.stream().filter(w -> w.worker().equals("W5")).findFirst().orElse(null);

        assertTrue(w1Final.tasksSize() >= 5, "W1 should maintain reasonable load after rebalancing");
        assertTrue(w3Final.tasksSize() >= 5, "W3 should maintain reasonable load after rebalancing");
        assertTrue(w4Final.tasksSize() >= 5, "W4 should maintain reasonable load after rebalancing");
        assertTrue(w5Final.tasksSize() >= 5, "W5 should maintain reasonable load after rebalancing");
    }
}
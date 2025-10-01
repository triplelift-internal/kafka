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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GlobalBalanceAssignorTest {

    private GlobalBalanceAssignor assignor;
    private MockTime time;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
    }

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
            assertTrue(worker.tasksSize() >= 1, "Each worker should have at least one task");
            assertTrue(worker.tasksSize() <= 3, "No worker should have more than 3 tasks");
        }
    }

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
            assertTrue(worker.connectorsSize() >= 1, "Each worker should have at least one connector");
            assertTrue(worker.connectorsSize() <= 2, "No worker should have more than 2 connectors");
        }
    }

    @Test
    public void testWorkerLoadUtilityMethods() {
        // Create a worker with mixed tasks
        WorkerLoad worker = new WorkerLoad.Builder("worker1")
            .withCopies(
                Arrays.asList("connector1", "connector2"),
                Arrays.asList(
                    new ConnectorTaskId("connector1", 0),
                    new ConnectorTaskId("connector1", 1),
                    new ConnectorTaskId("connector2", 0),
                    new ConnectorTaskId("connector3", 0)
                )
            ).build();

        // Test connector-specific task methods
        assertEquals(2, worker.taskCountForConnector("connector1"));
        assertEquals(1, worker.taskCountForConnector("connector2"));
        assertEquals(1, worker.taskCountForConnector("connector3"));
        assertEquals(0, worker.taskCountForConnector("nonexistent"));

        // Test unique connector count
        assertEquals(3, worker.uniqueConnectorTaskCount());

        // Test tasks for specific connector
        assertEquals(2, worker.tasksForConnector("connector1").size());
        assertEquals(1, worker.tasksForConnector("connector2").size());
        assertTrue(worker.tasksForConnector("nonexistent").isEmpty());
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
        assertEquals(taskCount, totalAssigned, "All tasks should be assigned");

        // Verify perfect balance constraint
        int[] taskCounts = workers.stream().mapToInt(WorkerLoad::tasksSize).toArray();
        int minTasks = Arrays.stream(taskCounts).min().orElse(0);
        int maxTasks = Arrays.stream(taskCounts).max().orElse(0);
        int difference = maxTasks - minTasks;

        assertTrue(difference <= 1, 
                  String.format("CRITICAL: Task balance violated! " +
                               "Tasks: %d, Workers: %d, Distribution: %s, " +
                               "Min: %d, Max: %d, Difference: %d (must be ≤ 1)",
                               taskCount, workerCount, Arrays.toString(taskCounts),
                               minTasks, maxTasks, difference));
    }
}
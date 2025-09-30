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
}
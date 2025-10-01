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

/**
 * Debug test to understand how the assignor works.
 */
public class GlobalBalanceAssignorDebugTest {

    private GlobalBalanceAssignor assignor;
    private MockTime time;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        assignor = new GlobalBalanceAssignor(new LogContext(), time, 0);
    }

    @Test
    public void testSimpleAssignment() {
        // Create workers with some existing tasks
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("W1").withCopies(
                Arrays.asList("C1"), 
                Arrays.asList(new ConnectorTaskId("C1", 0), new ConnectorTaskId("C1", 1))
            ).build(),
            new WorkerLoad.Builder("W2").withCopies(
                Arrays.asList("C1"), 
                Arrays.asList(new ConnectorTaskId("C1", 2))
            ).build(),
            new WorkerLoad.Builder("W3").withCopies(
                Arrays.asList(), Arrays.asList()
            ).build()
        );

        // Print initial state
        System.out.println("Initial state:");
        for (WorkerLoad worker : workers) {
            System.out.printf("Worker %s: %d tasks%n", worker.worker(), worker.tasksSize());
        }

        // Call assignTasks with NEW tasks only
        List<ConnectorTaskId> newTasks = Arrays.asList(
            new ConnectorTaskId("C1", 3),
            new ConnectorTaskId("C1", 4)
        );

        assignor.assignTasks(workers, newTasks);

        // Print final state
        System.out.println("\nFinal state:");
        int totalTasks = 0;
        for (WorkerLoad worker : workers) {
            totalTasks += worker.tasksSize();
            System.out.printf("Worker %s: %d tasks%n", worker.worker(), worker.tasksSize());
        }
        System.out.printf("Total tasks: %d (expected: 5)%n", totalTasks);
    }
}
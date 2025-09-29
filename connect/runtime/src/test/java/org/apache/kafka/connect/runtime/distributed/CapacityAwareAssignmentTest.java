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

import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for the capacity-aware task allocation enhancement.
 */
public class CapacityAwareAssignmentTest {

    @Test
    public void testWorkerLoadCapacityTracking() {
        WorkerLoad worker = new WorkerLoad.Builder("test-worker").build();
        
        // Initially should have no load
        assertEquals(0, worker.currentCpuLoad());
        assertTrue(worker.availableCpuCores() > 0);
        assertEquals(0.0, worker.effectiveCpuLoad(), 0.001);

        // Assign a task with weight 30 (moderate CPU usage)
        ConnectorTaskId task = new ConnectorTaskId("test-connector", 0);
        worker.assign(task, 30);
        
        assertEquals(30, worker.currentCpuLoad());
        assertEquals(30.0 / (worker.availableCpuCores() * 100), worker.effectiveCpuLoad(), 0.001);
        assertTrue(worker.tasks().contains(task));
    }

    @Test
    public void testCanAccommodateTask() {
        WorkerLoad worker = new WorkerLoad.Builder("test-worker").build();
        
        // Should be able to accommodate lightweight tasks
        assertTrue(worker.canAccommodateTask(1));
        assertTrue(worker.canAccommodateTask(50));
        assertTrue(worker.canAccommodateTask(100));
        
        // Fill up one CPU core (100 weight units)
        worker.assign(new ConnectorTaskId("test-1", 0), 100);
        
        // Should still accommodate tasks if there are more cores
        int totalCapacity = worker.availableCpuCores() * 100;
        if (totalCapacity > 100) {
            assertTrue(worker.canAccommodateTask(50));
        }
        
        // Fill up to near capacity
        int remainingCapacity = worker.remainingCpuCapacity();
        if (remainingCapacity > 1) {
            worker.assign(new ConnectorTaskId("test-2", 0), remainingCapacity - 1);
            
            // Should accommodate 1 more unit
            assertTrue(worker.canAccommodateTask(1));
            // Should not accommodate more than available
            assertFalse(worker.canAccommodateTask(2));
        }
    }

    @Test
    public void testCapacityAwareComparator() {
        WorkerLoad worker1 = new WorkerLoad.Builder("worker-1").build();
        WorkerLoad worker2 = new WorkerLoad.Builder("worker-2").build();
        
        // Initially, both workers should be equal
        List<WorkerLoad> workers = Arrays.asList(worker1, worker2);
        workers.sort(WorkerLoad.capacityAwareTaskComparator());
        
        // Add load to worker1 (moderate weight)
        worker1.assign(new ConnectorTaskId("test", 0), 30);
        
        // Now worker2 should be preferred (lower load)
        workers.sort(WorkerLoad.capacityAwareTaskComparator());
        assertEquals("worker-2", workers.get(0).worker());
        assertEquals("worker-1", workers.get(1).worker());
    }

    @Test
    public void testTasksWeightConfiguration() {
        // Test the configuration is properly defined
        assertEquals("tasks.weight", ConnectorConfig.TASKS_WEIGHT_CONFIG);
        assertEquals(1, ConnectorConfig.TASKS_WEIGHT_DEFAULT);
    }

    @Test
    public void testNewWeightScale() {
        WorkerLoad worker = new WorkerLoad.Builder("test-worker").build();
        
        // Test lightweight tasks (weight=1, many can fit on one core)
        for (int i = 0; i < 50 && worker.canAccommodateTask(1); i++) {
            worker.assign(new ConnectorTaskId("light-" + i, 0), 1);
        }
        assertTrue(worker.tasksSize() >= 50); // Should accommodate many lightweight tasks
        
        // Test heavy task (weight=100, takes full core)
        WorkerLoad worker2 = new WorkerLoad.Builder("test-worker-2").build();
        worker2.assign(new ConnectorTaskId("heavy", 0), 100);
        
        // If worker has only 1 core, it should be at capacity
        if (worker2.availableCpuCores() == 1) {
            assertFalse(worker2.canAccommodateTask(1));
        }
        
        // Test medium weight tasks
        WorkerLoad worker3 = new WorkerLoad.Builder("test-worker-3").build();
        worker3.assign(new ConnectorTaskId("medium-1", 0), 50);
        worker3.assign(new ConnectorTaskId("medium-2", 0), 50);
        
        // Two weight-50 tasks should fill one core
        assertEquals(100, worker3.currentCpuLoad());
    }

    @Test
    public void testBackwardCompatibilityAssignment() {
        // Test that workers still use original assignment when no weights are specified
        WorkerLoad worker1 = new WorkerLoad.Builder("worker-1").build();
        WorkerLoad worker2 = new WorkerLoad.Builder("worker-2").build();
        
        // Assign tasks using traditional method (no weight)
        worker1.assign(new ConnectorTaskId("test-1", 0));
        worker1.assign(new ConnectorTaskId("test-1", 1));
        
        worker2.assign(new ConnectorTaskId("test-2", 0));
        
        // Traditional comparator should still work
        List<WorkerLoad> workers = Arrays.asList(worker1, worker2);
        workers.sort(WorkerLoad.taskComparator());
        
        // worker2 has fewer tasks, so should be first
        assertEquals("worker-2", workers.get(0).worker());
        assertEquals("worker-1", workers.get(1).worker());
    }
}
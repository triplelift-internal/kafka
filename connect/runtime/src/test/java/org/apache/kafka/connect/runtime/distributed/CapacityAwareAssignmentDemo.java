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
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Demonstration of the capacity-aware task allocation enhancement.
 * This class shows how tasks with different weights are distributed across workers
 * based on CPU capacity rather than simple round-robin allocation.
 */
public class CapacityAwareAssignmentDemo {

    public static void main(String[] args) {
        System.out.println("=== Capacity-Aware Task Allocation Demo ===\n");

        // Simulate a cluster with 3 workers
        List<WorkerLoad> workers = Arrays.asList(
            new WorkerLoad.Builder("worker-1").build(),
            new WorkerLoad.Builder("worker-2").build(), 
            new WorkerLoad.Builder("worker-3").build()
        );

        // Print initial worker capacities
        System.out.println("Initial Worker Capacities:");
        for (WorkerLoad worker : workers) {
            System.out.printf("  %s: %d CPU cores available, total capacity: %d weight units, CPU load: %.2f\n", 
                worker.worker(), worker.availableCpuCores(), worker.totalCpuCapacity(), worker.effectiveCpuLoad());
        }

        // Simulate tasks with different weights
        System.out.println("\nTask Assignment Simulation:");
        
        // High-weight task (weight=100) - requires full CPU core
        ConnectorTaskId heavyTask = new ConnectorTaskId("heavy-connector", 0);
        WorkerLoad heavyWorker = findBestWorker(workers, 100);
        heavyWorker.assign(heavyTask, 100);
        System.out.printf("Assigned heavy task %s (weight=100) to %s\n", heavyTask, heavyWorker.worker());

        // Medium-weight tasks (weight=50) - half CPU core each
        for (int i = 0; i < 3; i++) {
            ConnectorTaskId mediumTask = new ConnectorTaskId("medium-connector", i);
            WorkerLoad mediumWorker = findBestWorker(workers, 50);
            mediumWorker.assign(mediumTask, 50);
            System.out.printf("Assigned medium task %s (weight=50) to %s\n", mediumTask, mediumWorker.worker());
        }

        // Light-weight tasks (weight=1) - 100 can share 1 CPU core
        for (int i = 0; i < 25; i++) {
            ConnectorTaskId lightTask = new ConnectorTaskId("light-connector", i);
            WorkerLoad lightWorker = findBestWorker(workers, 1);
            lightWorker.assign(lightTask, 1);
            if (i < 5) { // Only show first 5 assignments to avoid spam
                System.out.printf("Assigned light task %s (weight=1) to %s\n", lightTask, lightWorker.worker());
            } else if (i == 5) {
                System.out.println("  ... (continuing to assign remaining light tasks)");
            }
        }

        // Show final worker loads
        System.out.println("\nFinal Worker Loads:");
        for (WorkerLoad worker : workers) {
            System.out.printf("  %s: %d tasks, CPU load: %d/%d weight units (%.1f%%)\n", 
                worker.worker(), 
                worker.tasksSize(), 
                worker.currentCpuLoad(),
                worker.totalCpuCapacity(), 
                worker.effectiveCpuLoad() * 100);
        }

        System.out.println("\n=== Demo Complete ===");
        
        // Show the difference between old and new assignment
        demonstrateAssignmentComparison();
    }

    private static WorkerLoad findBestWorker(List<WorkerLoad> workers, int taskWeight) {
        workers.sort(WorkerLoad.capacityAwareTaskComparator());
        return workers.stream()
                .filter(worker -> worker.canAccommodateTask(taskWeight))
                .findFirst()
                .orElse(workers.get(0));
    }

    private static void demonstrateAssignmentComparison() {
        System.out.println("\n=== Assignment Strategy Comparison ===");
        System.out.println("Traditional Round-Robin:");
        System.out.println("  - Tasks distributed evenly by count");
        System.out.println("  - No consideration of task CPU requirements");
        System.out.println("  - Heavy tasks can overload workers");

        System.out.println("\nCapacity-Aware Assignment (1-100 scale):");
        System.out.println("  - Tasks distributed by CPU capacity weight");
        System.out.println("  - Weight 1: Lightweight (100 tasks per CPU core)");
        System.out.println("  - Weight 50: Moderate (2 tasks per CPU core)");
        System.out.println("  - Weight 100: Heavy (1 task per CPU core)");
        System.out.println("  - Prevents CPU contention and ensures performance");
        System.out.println("  - Backward compatible (weight 1 = traditional behavior)");
        System.out.println("  - Fine-grained control with 100-point scale");
    }
}
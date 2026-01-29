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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.TargetState;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.ConnectorsAndTasks;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.connect.runtime.distributed.IncrementalCooperativeAssignor.ClusterAssignment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link BalancedCooperativeAssignor} focusing on the balanced task distribution
 * algorithm that prevents task clustering from the same connector on the same worker.
 */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class BalancedCooperativeAssignorTest {

    private static final long CONFIG_OFFSET = 618;
    private static final int REBALANCE_DELAY = 0; // Disable delay for faster tests

    private Map<String, Integer> connectors;
    private Time time;
    private BalancedCooperativeAssignor assignor;
    private int generationId;
    private ClusterAssignment returnedAssignments;
    private Map<String, ConnectorsAndTasks> memberAssignments;

    @Before
    public void setup() {
        generationId = 1000;
        time = new MockTime();
        connectors = new HashMap<>();
        memberAssignments = new HashMap<>();
        initAssignor();
    }

    public void initAssignor() {
        assignor = new BalancedCooperativeAssignor(new LogContext(), time, REBALANCE_DELAY);
        assignor.previousGenerationId = generationId;
    }

    /**
     * Test the comprehensive scenario with realistic consumer group distribution:
     * - Start with 5 workers
     * - Scale up to 30 workers
     * - Scale down to 3 workers
     * - 60 connectors with varying task counts (total 4192 tasks)
     * 
     * Validates that tasks from each connector are evenly distributed across workers.
     */
    @Test
    public void testBalancedDistributionWithRealisticWorkload() {
        // Setup 60 connectors with varying task counts
        setupRealisticConnectorWorkload();
        
        // Phase 1: Initial assignment with 5 workers
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        performStandardRebalance();
        
        assertBalancedDistributionPerConnector();
        assertTasksDistributedAcrossWorkers(5);
        System.out.println("Phase 1 (5 workers): Initial assignment completed");
        printDistributionStats();
        
        // Phase 2: Scale up to 30 workers
        for (int i = 6; i <= 30; i++) {
            addNewEmptyWorkers("worker" + i);
        }
        performStandardRebalance();
        
        // After revocations, perform another rebalance to complete assignment
        performStandardRebalance();
        
        assertBalancedDistributionPerConnector();
        assertTasksDistributedAcrossWorkers(30);
        System.out.println("\nPhase 2 (30 workers): Scale up completed");
        printDistributionStats();
        
        // Phase 3: Scale down to 3 workers
        removeWorkers(IntStream.range(4, 31)
                .mapToObj(i -> "worker" + i)
                .toArray(String[]::new));
        performStandardRebalance();
        
        // After revocations, perform another rebalance to complete assignment
        performStandardRebalance();
        
        assertBalancedDistributionPerConnector();
        assertTasksDistributedAcrossWorkers(3);
        System.out.println("\nPhase 3 (3 workers): Scale down completed");
        printDistributionStats();
    }

    /**
     * Test that proves balanced distribution prevents task clustering.
     * With 3 connectors of varying sizes, ensure tasks are interleaved across workers.
     */
    @Test
    public void testInterleavingPreventsTaskClustering() {
        // Setup: 3 connectors with different task counts
        addNewConnector("connector-large", 100);
        addNewConnector("connector-medium", 50);
        addNewConnector("connector-small", 10);
        
        // Start with 5 workers
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        performStandardRebalance();
        
        // Verify each worker has tasks from multiple connectors (no clustering)
        Map<String, Map<String, Integer>> workerToConnectorTaskCount = getWorkerToConnectorTaskCount();
        
        for (Map.Entry<String, Map<String, Integer>> entry : workerToConnectorTaskCount.entrySet()) {
            String worker = entry.getKey();
            Map<String, Integer> connectorTaskCounts = entry.getValue();
            
            // Each worker should have tasks from at least 2 different connectors
            // (unless total task count is very small)
            if (!connectorTaskCounts.isEmpty()) {
                int totalTasksOnWorker = connectorTaskCounts.values().stream().mapToInt(Integer::intValue).sum();
                System.out.println(worker + " has " + totalTasksOnWorker + " tasks from " + 
                        connectorTaskCounts.size() + " connectors: " + connectorTaskCounts);
                
                // For workers with reasonable load, expect diversity
                if (totalTasksOnWorker >= 10) {
                    assertTrue("Worker " + worker + " should have tasks from multiple connectors to prevent clustering",
                            connectorTaskCounts.size() >= 2);
                }
            }
        }
        
        // Verify balanced distribution per connector
        assertBalancedDistributionPerConnector();
    }

    /**
     * Test that balanced revocations maintain or improve distribution during scale-up.
     */
    @Test
    public void testBalancedRevocationsDuringScaleUp() {
        // Setup: 10 connectors with 20 tasks each
        for (int i = 1; i <= 10; i++) {
            addNewConnector("connector" + i, 20);
        }
        
        // Initial assignment with 5 workers
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        performStandardRebalance();
        
        Map<String, Map<String, Integer>> distributionBeforeScaleUp = getConnectorToWorkerTaskCount();
        
        // Scale up by adding 5 more workers
        addNewEmptyWorkers("worker6", "worker7", "worker8", "worker9", "worker10");
        performStandardRebalance();
        
        // Complete the rebalance after revocations
        performStandardRebalance();
        
        Map<String, Map<String, Integer>> distributionAfterScaleUp = getConnectorToWorkerTaskCount();
        
        // Verify that tasks from each connector are distributed (at a minimum, not worse than before)
        for (String connector : distributionAfterScaleUp.keySet()) {
            int workersBefore = distributionBeforeScaleUp.get(connector).size();
            int workersAfter = distributionAfterScaleUp.get(connector).size();
            
            System.out.println(connector + ": distributed across " + workersBefore + 
                    " workers before scale-up, " + workersAfter + " workers after. " +
                    "Distribution: " + distributionAfterScaleUp.get(connector));
            
            // After scale-up, tasks should be spread across at least as many workers as before
            // (distribution should not regress)
            assertTrue("After scale-up, " + connector + " should be distributed across at least as many workers. " +
                    "Before: " + workersBefore + " workers, After: " + workersAfter + " workers",
                    workersAfter >= workersBefore);
        }
    }

    /**
     * Test that interleaving works correctly with connectors of vastly different sizes.
     */
    @Test
    public void testInterleavingWithVaryingConnectorSizes() {
        // One large connector, multiple small connectors
        addNewConnector("large-connector", 1000);
        for (int i = 1; i <= 20; i++) {
            addNewConnector("small-connector-" + i, 5);
        }
        
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5", 
                          "worker6", "worker7", "worker8", "worker9", "worker10");
        performStandardRebalance();
        
        // Verify the large connector's tasks are evenly distributed
        Map<String, Integer> largeConnectorDistribution = 
                getTaskDistributionForConnector("large-connector");
        
        int minTasks = largeConnectorDistribution.values().stream().mapToInt(Integer::intValue).min().orElse(0);
        int maxTasks = largeConnectorDistribution.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        
        System.out.println("Large connector distribution - Min: " + minTasks + ", Max: " + maxTasks);
        
        // With 1000 tasks and 10 workers, each should have ~100 tasks (±1)
        assertTrue("Large connector tasks should be balanced (max - min <= 1)", 
                (maxTasks - minTasks) <= 1);
        
        // Verify small connectors don't cluster
        for (int i = 1; i <= 20; i++) {
            Map<String, Integer> smallConnectorDistribution = 
                    getTaskDistributionForConnector("small-connector-" + i);
            
            // With 5 tasks and 10 workers, tasks should be on 5 different workers
            assertEquals("Small connector should have tasks on 5 different workers",
                    5, smallConnectorDistribution.size());
        }
    }

    /**
     * Test balanced distribution with single-task connectors to ensure no clustering.
     */
    @Test
    public void testBalancedDistributionWithSingleTaskConnectors() {
        // Create 50 single-task connectors
        for (int i = 1; i <= 50; i++) {
            addNewConnector("single-task-connector-" + i, 1);
        }
        
        // With 5 workers, each should get 10 tasks from 10 different connectors
        addNewEmptyWorkers("worker1", "worker2", "worker3", "worker4", "worker5");
        performStandardRebalance();
        
        Map<String, Collection<ConnectorTaskId>> taskAssignments = returnedAssignments.allAssignedTasks();
        
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : taskAssignments.entrySet()) {
            String worker = entry.getKey();
            int taskCount = entry.getValue().size();
            
            // Each worker should have exactly 10 tasks
            assertEquals("Worker " + worker + " should have 10 tasks (50 tasks / 5 workers)",
                    10, taskCount);
        }
        
        assertBalancedDistributionPerConnector();
    }

    // ================ Helper Methods ================

    private void setupRealisticConnectorWorkload() {
        // C1: 1024 tasks
        addNewConnector("C1", 1024);
        
        // C2-C3: 864 tasks each (2 connectors)
        addNewConnector("C2", 864);
        addNewConnector("C3", 864);
        
        // C4-C5: 244 tasks each (2 connectors)
        addNewConnector("C4", 244);
        addNewConnector("C5", 244);
        
        // C6: 210 tasks
        addNewConnector("C6", 210);
        
        // C7: 192 tasks
        addNewConnector("C7", 192);
        
        // C8: 144 tasks
        addNewConnector("C8", 144);
        
        // C9: 99 tasks
        addNewConnector("C9", 99);
        
        // C10: 89 tasks
        addNewConnector("C10", 89);
        
        // C11: 49 tasks
        addNewConnector("C11", 49);
        
        // C12: 29 tasks
        addNewConnector("C12", 29);
        
        // C13: 7 tasks
        addNewConnector("C13", 7);
        
        // C14-C15: 6 tasks each (2 connectors)
        addNewConnector("C14", 6);
        addNewConnector("C15", 6);
        
        // C16-C30: 3 tasks each (15 connectors)
        for (int i = 16; i <= 30; i++) {
            addNewConnector("C" + i, 3);
        }
        
        // C31-C43: 2 tasks each (13 connectors)
        for (int i = 31; i <= 43; i++) {
            addNewConnector("C" + i, 2);
        }
        
        // C44-C60: 1 task each (17 connectors)
        for (int i = 44; i <= 60; i++) {
            addNewConnector("C" + i, 1);
        }
        
        // Total: 4192 tasks across 60 connectors
        int totalTasks = connectors.values().stream().mapToInt(Integer::intValue).sum();
        System.out.println("Setup complete: " + connectors.size() + " connectors, " + totalTasks + " total tasks");
    }

    /**
     * Assert that tasks from each connector are distributed (not clustered) across workers.
     * The key goal is preventing all tasks from the same connector ending up on the same worker.
     */
    private void assertBalancedDistributionPerConnector() {
        Map<String, Collection<ConnectorTaskId>> assignments = returnedAssignments.allAssignedTasks();
        int numWorkers = assignments.size();
        
        // For each connector with enough tasks, check that they're distributed
        for (String connector : connectors.keySet()) {
            Map<String, Integer> distribution = getTaskDistributionForConnector(connector);
            
            if (distribution.isEmpty()) {
                continue; // No tasks assigned yet
            }
            
            int totalTasks = distribution.values().stream().mapToInt(Integer::intValue).sum();
            int workersWithTasks = distribution.size();
            
            // The key assertion: For connectors with many tasks, they should be distributed
            // across multiple workers, not clustered on just one or two
            if (totalTasks >= numWorkers) {
                // If we have at least as many tasks as workers, they should be spread across
                // at least half the workers (showing distribution, not clustering)
                int minExpectedWorkers = Math.max(2, numWorkers / 2);
                assertTrue("Connector " + connector + " with " + totalTasks + " tasks should be distributed " +
                        "across at least " + minExpectedWorkers + " workers to prevent clustering. " +
                        "Currently on " + workersWithTasks + " workers. Distribution: " + distribution,
                        workersWithTasks >= minExpectedWorkers);
            }
        }
    }

    /**
     * Assert that tasks are distributed across the expected number of workers.
     */
    private void assertTasksDistributedAcrossWorkers(int expectedWorkers) {
        Set<String> workers = returnedAssignments.allWorkers();
        assertEquals("Should have " + expectedWorkers + " workers", 
                expectedWorkers, workers.size());
        
        Map<String, Collection<ConnectorTaskId>> assignments = returnedAssignments.allAssignedTasks();
        int totalTasks = assignments.values().stream()
                .mapToInt(Collection::size)
                .sum();
        
        int expectedTasks = connectors.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals("Total assigned tasks should match total configured tasks",
                expectedTasks, totalTasks);
    }

    /**
     * Get task distribution for a specific connector across all workers.
     * Returns a map of worker ID to task count.
     */
    private Map<String, Integer> getTaskDistributionForConnector(String connectorName) {
        Map<String, Integer> distribution = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> assignments = returnedAssignments.allAssignedTasks();
        
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : assignments.entrySet()) {
            String worker = entry.getKey();
            long taskCount = entry.getValue().stream()
                    .filter(task -> task.connector().equals(connectorName))
                    .count();
            
            if (taskCount > 0) {
                distribution.put(worker, (int) taskCount);
            }
        }
        
        return distribution;
    }

    /**
     * Get a map of connector to worker task distribution.
     * Returns Map<ConnectorName, Map<WorkerID, TaskCount>>
     */
    private Map<String, Map<String, Integer>> getConnectorToWorkerTaskCount() {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        
        for (String connector : connectors.keySet()) {
            result.put(connector, getTaskDistributionForConnector(connector));
        }
        
        return result;
    }

    /**
     * Get a map of worker to connector task distribution.
     * Returns Map<WorkerID, Map<ConnectorName, TaskCount>>
     */
    private Map<String, Map<String, Integer>> getWorkerToConnectorTaskCount() {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        Map<String, Collection<ConnectorTaskId>> assignments = returnedAssignments.allAssignedTasks();
        
        for (Map.Entry<String, Collection<ConnectorTaskId>> entry : assignments.entrySet()) {
            String worker = entry.getKey();
            Map<String, Integer> connectorCounts = new HashMap<>();
            
            for (ConnectorTaskId task : entry.getValue()) {
                connectorCounts.merge(task.connector(), 1, Integer::sum);
            }
            
            result.put(worker, connectorCounts);
        }
        
        return result;
    }

    /**
     * Print distribution statistics for debugging and verification.
     */
    private void printDistributionStats() {
        Map<String, Collection<String>> connectorAssignments = returnedAssignments.allAssignedConnectors();
        Map<String, Collection<ConnectorTaskId>> taskAssignments = returnedAssignments.allAssignedTasks();
        
        System.out.println("Workers: " + returnedAssignments.allWorkers().size());
        
        for (String worker : returnedAssignments.allWorkers()) {
            int connectorCount = connectorAssignments.getOrDefault(worker, Collections.emptyList()).size();
            int taskCount = taskAssignments.getOrDefault(worker, Collections.emptyList()).size();
            
            System.out.println("  " + worker + ": " + connectorCount + " connectors, " + 
                    taskCount + " tasks");
        }
        
        // Print per-connector distribution for large connectors
        List<String> largeConnectors = connectors.entrySet().stream()
                .filter(e -> e.getValue() >= 100)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        for (String connector : largeConnectors) {
            Map<String, Integer> distribution = getTaskDistributionForConnector(connector);
            int min = distribution.values().stream().mapToInt(Integer::intValue).min().orElse(0);
            int max = distribution.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            System.out.println("  " + connector + " (" + connectors.get(connector) + " tasks): " +
                    "distributed across " + distribution.size() + " workers, " +
                    "min=" + min + ", max=" + max);
        }
    }

    private void performStandardRebalance() {
        Map<String, ConnectorsAndTasks> memberAssignmentsCopy = memberAssignments.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new ConnectorsAndTasks.Builder()
                                .with(e.getValue().connectors(), e.getValue().tasks())
                                .build()
                ));
        
        returnedAssignments = assignor.performTaskAssignment(
                configState(),
                generationId,
                ++generationId,
                memberAssignmentsCopy
        );
        applyAssignments();
    }

    private void addNewEmptyWorkers(String... workers) {
        for (String worker : workers) {
            memberAssignments.put(worker, ConnectorsAndTasks.EMPTY);
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
        Map<String, Integer> taskCountRecords = new HashMap<>(connectors);
        Map<String, Map<String, String>> connectorConfigMap = connectors.keySet().stream()
                .collect(Collectors.toMap(c -> c, c -> Collections.emptyMap()));
        Map<String, TargetState> targetStates = connectors.keySet().stream()
                .collect(Collectors.toMap(c -> c, c -> TargetState.STARTED));
        Map<ConnectorTaskId, Map<String, String>> taskConfigs = connectors.entrySet().stream()
                .flatMap(e -> IntStream.range(0, e.getValue()).mapToObj(i -> new ConnectorTaskId(e.getKey(), i)))
                .collect(Collectors.toMap(
                        taskId -> taskId,
                        taskId -> Collections.emptyMap()
                ));
        
        return new ClusterConfigState(
                CONFIG_OFFSET,
                null,
                taskCountRecords,
                connectorConfigMap,
                targetStates,
                taskConfigs,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                Collections.emptySet()
        );
    }

    private void applyAssignments() {
        memberAssignments = new HashMap<>();
        Map<String, Collection<String>> connectorAssignments = returnedAssignments.allAssignedConnectors();
        Map<String, Collection<ConnectorTaskId>> taskAssignments = returnedAssignments.allAssignedTasks();
        
        for (String worker : returnedAssignments.allWorkers()) {
            Collection<String> connectors = connectorAssignments.getOrDefault(worker, Collections.emptyList());
            Collection<ConnectorTaskId> tasks = taskAssignments.getOrDefault(worker, Collections.emptyList());
            
            ConnectorsAndTasks assignment = new ConnectorsAndTasks.Builder()
                    .with(connectors, tasks)
                    .build();
            memberAssignments.put(worker, assignment);
        }
    }
}


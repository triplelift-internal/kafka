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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.distributed.WorkerCoordinator.WorkerLoad;
import org.apache.kafka.connect.util.ConnectorTaskId;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An assignor that prioritizes global balance of tasks over availability and continuity.
 * This assignor extends the incremental cooperative assignor to leverage existing 
 * rebalancing logic while implementing a global balance strategy for task assignment.
 * 
 * <p>Tasks for the same connector are spread evenly across all available worker nodes
 * rather than being placed together, providing better load distribution but potentially
 * impacting connector locality.
 * 
 * <p>This assignor maintains all existing features like delayed rebalancing, revocation
 * handling, and incremental cooperative rebalancing while changing only the assignment
 * strategy for new tasks and connectors.
 */
public class GlobalBalanceAssignor extends IncrementalCooperativeAssignor {
    private final Logger log;
    
    public GlobalBalanceAssignor(LogContext logContext, Time time, int maxDelay) {
        super(logContext, time, maxDelay);
        this.log = logContext.logger(GlobalBalanceAssignor.class);
    }
    
    @Override
    protected void assignTasks(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        log.debug("Assigning {} tasks with global balance strategy across {} workers", 
                  tasks.size(), workerAssignment.size());
        assignTasksWithGlobalBalance(workerAssignment, tasks);
    }
    
    @Override
    protected void assignConnectors(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        log.debug("Assigning {} connectors with global balance strategy across {} workers", 
                  connectors.size(), workerAssignment.size());
        assignConnectorsWithGlobalBalance(workerAssignment, connectors);
    }
    
    /**
     * Assigns tasks with global balance strategy. Tasks are grouped by connector
     * and each connector's tasks are distributed evenly across all workers.
     */
    private void assignTasksWithGlobalBalance(List<WorkerLoad> workerAssignment, Collection<ConnectorTaskId> tasks) {
        if (tasks.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        // Group tasks by connector
        Map<String, List<ConnectorTaskId>> tasksByConnector = groupTasksByConnector(tasks);
        
        log.debug("Grouped {} tasks into {} connector groups: {}", 
                  tasks.size(), tasksByConnector.size(), tasksByConnector.keySet());
        
        // For each connector group, distribute tasks evenly across workers
        for (Map.Entry<String, List<ConnectorTaskId>> entry : tasksByConnector.entrySet()) {
            String connectorName = entry.getKey();
            List<ConnectorTaskId> connectorTasks = entry.getValue();
            
            log.debug("Distributing {} tasks for connector {} across {} workers", 
                      connectorTasks.size(), connectorName, workerAssignment.size());
            
            // Sort workers by current task load for balanced assignment
            workerAssignment.sort(WorkerLoad.taskComparator());
            
            // Distribute tasks for this connector across all workers
            distributeConnectorTasksGlobally(workerAssignment, connectorTasks);
        }
    }
    
    /**
     * Assigns connectors with global balance strategy, ensuring even distribution
     * of connectors across workers.
     */
    private void assignConnectorsWithGlobalBalance(List<WorkerLoad> workerAssignment, Collection<String> connectors) {
        if (connectors.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        // Convert to list for indexed access
        List<String> connectorList = connectors.stream().collect(Collectors.toList());
        
        // Round-robin assignment of connectors
        for (int i = 0; i < connectorList.size(); i++) {
            // Sort workers by connector load to maintain balance
            workerAssignment.sort(WorkerLoad.connectorComparator());
            
            String connector = connectorList.get(i);
            WorkerLoad leastLoadedWorker = workerAssignment.get(0);
            
            log.debug("Assigning connector {} to worker {} (current connector load: {})", 
                      connector, leastLoadedWorker.worker(), leastLoadedWorker.connectorsSize());
            
            leastLoadedWorker.assign(connector);
        }
    }
    
    /**
     * Groups tasks by their connector name.
     */
    private Map<String, List<ConnectorTaskId>> groupTasksByConnector(Collection<ConnectorTaskId> tasks) {
        return tasks.stream()
                .collect(Collectors.groupingBy(ConnectorTaskId::connector));
    }
    
    /**
     * Distributes a connector's tasks globally across all workers using round-robin.
     * This ensures that tasks for the same connector are spread evenly rather than
     * being co-located on the same worker.
     */
    private void distributeConnectorTasksGlobally(List<WorkerLoad> workerAssignment, List<ConnectorTaskId> connectorTasks) {
        if (connectorTasks.isEmpty() || workerAssignment.isEmpty()) {
            return;
        }

        // Round-robin distribution across all workers for this connector's tasks
        int workerIndex = 0;
        for (ConnectorTaskId task : connectorTasks) {
            WorkerLoad worker = workerAssignment.get(workerIndex);
            
            log.debug("Assigning task {} to worker {} (current task load: {})", 
                      task, worker.worker(), worker.tasksSize());
            
            worker.assign(task);
            workerIndex = (workerIndex + 1) % workerAssignment.size();
        }
    }
}
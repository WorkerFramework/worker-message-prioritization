/*
 * Copyright 2022-2023 Open Text.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.workerframework.workermessageprioritization.redistribution.lowlevel;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.rabbitmq.client.Connection;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Moves messages from staging queues to the target queue
 */
public class StagingTargetPairProvider {

    public StagingTargetPairProvider() {
    }
    
    public Set<StagingQueueTargetQueuePair> provideStagingTargetPairs(
            final Connection connection,
            final DistributorWorkItem distributorWorkItem,
            final Map<Queue, Long> consumptionTargets) {

        final Set<StagingQueueTargetQueuePair> stagingQueueTargetQueuePairs = new HashSet<>();
        
        final long overallConsumptionTarget = consumptionTargets.values().stream().mapToLong(Long::longValue).sum();
        
        if(overallConsumptionTarget <= 0) {
            return stagingQueueTargetQueuePairs;
        }
        
        for(final Queue stagingQueue: distributorWorkItem.getStagingQueues()) {
            
            final Long consumptionTarget = consumptionTargets.get(stagingQueue);

            final StagingQueueTargetQueuePair stagingQueueTargetQueuePair = 
                    new StagingQueueTargetQueuePair(connection, 
                            stagingQueue, distributorWorkItem.getTargetQueue(),
                            consumptionTarget);

            stagingQueueTargetQueuePairs.add(stagingQueueTargetQueuePair);
        }
        return stagingQueueTargetQueuePairs;
    }
}

/*
 * Copyright 2022-2026 Open Text.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Moves messages from staging queues to the target queue
 */
public class StagingTargetPairProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingTargetPairProvider.class);

    public StagingTargetPairProvider() {
    }
    
    public Set<StagingQueueTargetQueuePair> provideStagingTargetPairs(
            final Connection connection,
            final DistributorWorkItem distributorWorkItem,
            final Map<Queue, Long> consumptionTargets,
            final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds) {

        final Set<StagingQueueTargetQueuePair> stagingQueueTargetQueuePairs = new HashSet<>();
        
        final long overallConsumptionTarget = consumptionTargets.values().stream().mapToLong(Long::longValue).sum();
        
        if(overallConsumptionTarget <= 0) {
            LOGGER.debug("Not creating any StagingQueueTargetQueuePairs as the overallConsumptionTarget is <= 0: {}",
                    overallConsumptionTarget);
            return stagingQueueTargetQueuePairs;
        }
        
        for(final Queue stagingQueue: distributorWorkItem.getStagingQueues()) {
            
            final Long consumptionTarget = consumptionTargets.get(stagingQueue);

            if (consumptionTarget == null || consumptionTarget <= 0) {
                LOGGER.debug("Not creating a StagingQueueTargetQueuePair for staging queue '{}' and target queue '{}' " +
                                "as the consumption target is null or <= 0: {}",
                        stagingQueue.getName(),
                        distributorWorkItem.getTargetQueue().getName(),
                        consumptionTarget);
                continue;
            }

            final StagingQueueTargetQueuePair stagingQueueTargetQueuePair = 
                    new StagingQueueTargetQueuePair(connection, 
                            stagingQueue, distributorWorkItem.getTargetQueue(),
                            consumptionTarget, consumerPublisherPairLastDoneWorkTimeoutMilliseconds);

            stagingQueueTargetQueuePairs.add(stagingQueueTargetQueuePair);
        }
        return stagingQueueTargetQueuePairs;
    }
}

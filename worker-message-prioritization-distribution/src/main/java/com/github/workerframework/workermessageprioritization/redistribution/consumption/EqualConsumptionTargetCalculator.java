/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Attempts to send an equal number of message from each staging queue to the target queue
 */
public class EqualConsumptionTargetCalculator implements ConsumptionTargetCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(EqualConsumptionTargetCalculator.class);
    private final TargetQueueCapacityProvider targetQueueCapacityProvider;

    public EqualConsumptionTargetCalculator(final TargetQueueCapacityProvider targetQueueCapacityProvider) {

        this.targetQueueCapacityProvider = targetQueueCapacityProvider;
    }
    
    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem) {
        
        final long targetQueueCapacity = targetQueueCapacityProvider.get(distributorWorkItem.getTargetQueue());
        
        final long lastKnownTargetQueueLength = distributorWorkItem.getTargetQueue().getMessages();

        final long totalKnownPendingMessages =
                distributorWorkItem.getStagingQueues().stream()
                        .map(Queue::getMessages).mapToLong(Long::longValue).sum();

        final long consumptionTarget = targetQueueCapacity - lastKnownTargetQueueLength;
        final long sourceQueueConsumptionTarget;
        if(distributorWorkItem.getStagingQueues().isEmpty()) {
            sourceQueueConsumptionTarget = 0;
        }
        else {
            sourceQueueConsumptionTarget = (long) Math.ceil((double)consumptionTarget /
                    distributorWorkItem.getStagingQueues().size());
        }

        LOGGER.info("TargetQueue {}, {} messages, SourceQueues {}, {} messages, " +
                        "Overall consumption target: {}, Individual Source Queue consumption target: {}",
                distributorWorkItem.getTargetQueue().getName(), lastKnownTargetQueueLength,
                (long) distributorWorkItem.getStagingQueues().size(), totalKnownPendingMessages,
                consumptionTarget, sourceQueueConsumptionTarget);
        
        return distributorWorkItem.getStagingQueues().stream()
                .collect(Collectors.toMap(q -> q, q-> sourceQueueConsumptionTarget));
        
    }
}

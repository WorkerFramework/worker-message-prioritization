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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Attempts to send an equal (roughly) number of message from each staging queue to the target queue.
 *
 * Examples:
 *
 * Target queue capacity: 100
 * Staging queue 1 contains 200 messages
 * Staging queue 2 contains 300 messages
 * Result: 50 messages will be sent from staging queue 1 and 50 messages will be sent from staging queue 2
 *
 * Target queue capacity: 100
 * Staging queue 1 contains 200 messages
 * Staging queue 2 contains 10 messages
 * Result: 50 messages will be sent from staging queue 1 and 10 messages will be sent from staging queue 2
 */
public class EqualConsumptionTargetCalculator extends ConsumptionTargetCalculatorBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(EqualConsumptionTargetCalculator.class);

    @Inject
    public EqualConsumptionTargetCalculator(final TargetQueueSettingsProvider targetQueueSettingsProvider,
                                            final CapacityCalculatorBase capacityCalculatorBase) {
        super(targetQueueSettingsProvider, capacityCalculatorBase);
    }
    
    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem) {

        // The number of messages the target queue has capacity for
        final long targetQueueCapacity = getTargetQueueCapacity(distributorWorkItem.getTargetQueue());

        // The total number of messages in all the staging queues
        final long numMessagesInStagingQueues =
                distributorWorkItem.getStagingQueues().stream()
                        .map(Queue::getMessages).mapToLong(Long::longValue).sum();

        // The maximum number of messages that can be consumed from each staging queue
        final long maxNumMessagesToConsumeFromStagingQueue;
        if(distributorWorkItem.getStagingQueues().isEmpty()) {
            maxNumMessagesToConsumeFromStagingQueue = 0;
        }
        else {
            maxNumMessagesToConsumeFromStagingQueue =
                    (long) Math.ceil((double)targetQueueCapacity / distributorWorkItem.getStagingQueues().size());
        }

        // Return value
        final Map<Queue, Long> stagingQueueToConsumptionTargetMap = new HashMap<>();

        // Set the consumption target for a staging queue to the number of messages in the staging queue OR the maximum amount of
        // messages we are allowed to consume from the staging queue (whichever is lower).
        // This ensures that a consumption target is always hit, allowing a staging queue consumer to be closed/removed and not left
        // running waiting for more messages to arrive in the staging queue.
        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {

            final long numMessagesInStagingQueue = stagingQueue.getMessages();

            final long actualNumberOfMessagesToConsumeFromStagingQueue =
                    Math.min(numMessagesInStagingQueue, maxNumMessagesToConsumeFromStagingQueue);

            LOGGER.debug("Staging queue {} contains {} messages, " +
                            "the maximum number of messages to consume from each staging queue is {}, " +
                            "setting the consumption target for this staging queue to the minimum of these 2 values: {}",
                    stagingQueue.getName(),
                    numMessagesInStagingQueue,
                    maxNumMessagesToConsumeFromStagingQueue,
                    actualNumberOfMessagesToConsumeFromStagingQueue
            );

            stagingQueueToConsumptionTargetMap.put(stagingQueue, actualNumberOfMessagesToConsumeFromStagingQueue);
        }

        final Queue targetQueue = distributorWorkItem.getTargetQueue();

        LOGGER.debug("Number of messages in target queue {}: {}, " +
                        "Target queue capacity is: {}, " +
                        "Number of staging queues: {}, " +
                        "Total number of messages in all staging queues: {}, " +
                        "Staging queue consumption targets: {}",
                targetQueue.getName(),
                targetQueue.getMessages(),
                targetQueueCapacity,
                (long) distributorWorkItem.getStagingQueues().size(),
                numMessagesInStagingQueues,
                stagingQueueToConsumptionTargetMap);

        return stagingQueueToConsumptionTargetMap;
    }
}

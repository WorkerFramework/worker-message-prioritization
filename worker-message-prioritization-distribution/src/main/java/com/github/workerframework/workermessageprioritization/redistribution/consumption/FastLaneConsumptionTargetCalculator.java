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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FastLaneConsumptionTargetCalculator extends MinimumConsumptionTargetCalculator {
    private static final Logger LOGGER = LoggerFactory.getLogger(EqualConsumptionTargetCalculator.class);

    public FastLaneConsumptionTargetCalculator(final TargetQueueSettingsProvider targetQueueSettingsProvider) {
        super(targetQueueSettingsProvider);
    }

    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem) {

        // The number of messages the target queue has capacity for.
        final long targetQueueCapacity = getTargetQueueCapacity(distributorWorkItem.getTargetQueue());

        final StagingQueueUnusedMessageConsumptionCalculator stagingQueueUnusedMessageConsumptionCalculator =
                new StagingQueueUnusedMessageConsumptionCalculator(distributorWorkItem);

        final StagingQueueWeightCalculator stagingQueueWeightCalculator = new StagingQueueWeightCalculator(distributorWorkItem);

        // The total number of messages in all the staging queues.
        final long numMessagesInStagingQueues =
                distributorWorkItem.getStagingQueues().stream()
                        .map(Queue::getMessages).mapToLong(Long::longValue).sum();

        // The total number of weights across all staging queues.
        final double stagingQueueWeight = stagingQueueWeightCalculator.calculateTotalStagingQueueWeight();

        // Target Queue capacity available per weight.
        final double capacityPerWeight = targetQueueCapacity / stagingQueueWeight;

        // Calculates, in terms of weights, the unused messages caused by staging queues smaller than the available target queue
        // capacity per staging queue.
        final double unusedWeightToDistribute =
                stagingQueueUnusedMessageConsumptionCalculator.calculateStagingQueueUnusedWeight(targetQueueCapacity, stagingQueueWeight);

        // Map staging queue to corresponding weight.
        final Map<Queue, Long> stagingQueueToConsumptionTargetMap = new HashMap<>();
        final Map<Queue, Double> stagingQueueWeightMap = stagingQueueWeightCalculator.getStagingQueueWeights();

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {

            // The maximum number of messages that can be consumed from each staging queue
            // This is calculated by multiplying the capacity available per weight, by the original weight with any unused weight added
            // on.
            // For smaller staging queues the weight will still be increased, however the extra space will not be used.
            final double maxNumMessagesToConsumeFromStagingQueue =
                    Math.ceil(capacityPerWeight * (stagingQueueWeightMap.get(stagingQueue) + unusedWeightToDistribute));

            final long numMessagesInStagingQueue = stagingQueue.getMessages();

            // Staging queues with less messages on the queue than the maxNumMessagesToConsumeFromStagingQueue, will be set to consume
            // ONLY the messages on their queue. The unusedWeightToDistribute will increase the rest of the queues to pick up the rest
            // of the space left.
            final long actualNumberOfMessagesToConsumeFromStagingQueue =
                    (long) Math.min(numMessagesInStagingQueue, maxNumMessagesToConsumeFromStagingQueue);

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

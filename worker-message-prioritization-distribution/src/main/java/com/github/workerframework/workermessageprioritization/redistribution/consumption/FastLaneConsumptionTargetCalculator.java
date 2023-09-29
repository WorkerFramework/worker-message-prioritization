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
import com.google.common.util.concurrent.AtomicDouble;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
public class FastLaneConsumptionTargetCalculator extends ConsumptionTargetCalculatorBase {
    private static final Logger FAST_LANE_LOGGER = LoggerFactory.getLogger("FAST_LANE");
    private final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider;

    @Inject
    public FastLaneConsumptionTargetCalculator(final TargetQueueSettingsProvider targetQueueSettingsProvider,
                                               final CapacityCalculatorBase capacityCalculatorBase,
                                               final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider) {
        super(targetQueueSettingsProvider, capacityCalculatorBase);
        this.stagingQueueWeightSettingsProvider = stagingQueueWeightSettingsProvider;
    }

    @Override
    public Map<Queue, Long> calculateConsumptionTargets(final DistributorWorkItem distributorWorkItem) {

        FAST_LANE_LOGGER.debug("Using Fast lane consumption target calculator");

        // The number of messages the target queue has capacity for.
        double targetQueueCapacity = getTargetQueueCapacity(distributorWorkItem.getTargetQueue());

        // Get a list of the staging queue names. This will be used to find the weight of the queue.
        final List<String> stagingQueueNames =
                distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

        // Map the name of each staging queue to its weight
        final Map<String, Double> stagingQueueWeightMap =
                stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames);

        // The total number of messages in all the staging queues.
        double availableMessages = distributorWorkItem.getStagingQueues().stream()
                .mapToLong(Queue::getMessages).sum();

        // Map the staging queue to the consumption target.
        final Map<Queue, AtomicDouble> stagingQueueToConsumptionTargetMap = distributorWorkItem.getStagingQueues().stream()
                .collect(Collectors.toMap(Function.identity(), q -> new AtomicDouble(0)));

        // Initially this list will hold all staging queues and their messages.
        // Once an initial scan of all staging queues have been completed below, this value will decrease based
        // off any queues who's messages have all been covered using the target queue capacity available.
        // Staging queues will only stay in this list if they have more messages on their queue than the initial target queue
        // capacity that was provided.
        final List<Queue> stagingQueuesWithSpareMessages = distributorWorkItem.getStagingQueues().stream()
                .filter(q -> q.getMessages() > stagingQueueToConsumptionTargetMap.get(q).longValue())
                .collect(Collectors.toCollection(ArrayList::new));

        // Will continue in this loop until either there are no available messages on any of the staging queue
        // Eg: The total combined messages in the staging queues are less than the total target queue capacity
        // OR the entire target queue capacity has been used.
        while(availableMessages > 0 && targetQueueCapacity > 0) {

            // The total number of weights across all staging queues.
            // For now this is just setting all the weights to 1.
            final double totalStagingQueueWeight = stagingQueueWeightMap.values().stream().mapToDouble(Double::doubleValue).sum();

            // Target Queue capacity available per weight.
            // TargetQueueCapacity and totalStagingQueueWeight will reduce while in this loop as staging queues
            // that have all messages used up are removed from stagingQueuesWithSpareMessages.
            final double messagesPerWeight = targetQueueCapacity / totalStagingQueueWeight;

            // An empty list to store staging queues once each has used the target queue capacity to its full potential.
            final List<Queue> completedStagingQueues = new ArrayList<>();

            // Initially this will loop over each staging queue
            // Once this has been completed once it will only go back over if there are still staging queues with spare messages
            // In other words staging queues which had more messages in their staging queue than the portion of target queue provided.
            if(targetQueueCapacity < stagingQueueWeightMap.size()){
                targetQueueCapacity = 0;
            }
            for(final Queue stagingQueue: stagingQueuesWithSpareMessages) {

                double stagingQueueConsumption = 0;

                // Multiply the messages available to the staging queue by its weight to get the full staging queue consumption.
                // When the target queue capacity is smaller than the number of staging queues the value per queue will be less than 0.
                // Therefore, we need to round up rather than down at this point.
                if(targetQueueCapacity < stagingQueueWeightMap.size()) {
                    stagingQueueConsumption =
                            Math.ceil(messagesPerWeight * stagingQueueWeightMap.get(stagingQueue.getName()));
                }else{
                    stagingQueueConsumption =
                            Math.floor(messagesPerWeight * stagingQueueWeightMap.get(stagingQueue.getName()));
                }

                // The difference in total messages on the staging queue and number of messages already being consumed by this staging
                // queue.
                // On the first scan of this for each staging queue the value in the stagingQueueToConsumptionTargetMap will be 0.
                // This difference will only return a value less than the total messages on the staging queue when this staging queue
                // has been scanned once but still has messages available that could use any leftover messages.
                final double stagingQueueAvailable = stagingQueue.getMessages() -
                        stagingQueueToConsumptionTargetMap.get(stagingQueue).longValue();

                // Set the staging queue consumption to the smaller value of either the total number of messages in the queue or the
                // target queue capacity available to the staging queue.
                stagingQueueConsumption = Math.min(stagingQueueConsumption, stagingQueueAvailable);

                // This stores the messages consumed by the staging queue. If there is spare capacity on the target queue
                // This will add any additional messages available to the staging queue.
                stagingQueueToConsumptionTargetMap.get(stagingQueue).addAndGet(stagingQueueConsumption);

                // Remove the used messages from the total number of available messages for that staging queue.
                availableMessages = availableMessages - stagingQueueConsumption;

                // Remove the used messages from the total number of available target queue messages.
                targetQueueCapacity = targetQueueCapacity - stagingQueueConsumption;

                // If there are less or equal number of messages in the staging queue than the number of messages it is given to consume,
                // then the queue will be removed from the weight map as it will no longer be considered for extra available weight.
                // This staging queue is also added to the list of completed staging queues as all of its available messages
                // have been processed.
                if(stagingQueue.getMessages() <= stagingQueueToConsumptionTargetMap.get(stagingQueue).longValue()) {
                    stagingQueueWeightMap.remove(stagingQueue.getName());
                    completedStagingQueues.add(stagingQueue);
                }
            }
            // Remove any staging queues that have been marked as completed from the list of staging queues
            // which are stored to keep track of staging queues with more messages on their queue than the capacity of
            // target queue provided.
            stagingQueuesWithSpareMessages.removeAll(completedStagingQueues);
        }

        final Queue targetQueue = distributorWorkItem.getTargetQueue();

        for (final Queue stagingQueue : distributorWorkItem.getStagingQueues()) {
            FAST_LANE_LOGGER.debug("{}, number of messages: {}", stagingQueue.getName(),
                    stagingQueueToConsumptionTargetMap.get(stagingQueue));
        }

        FAST_LANE_LOGGER.debug("Available messages total: {}", availableMessages);

        // Return the mapped staging queue to the number of messages it will consume from the target queue
        return stagingQueueToConsumptionTargetMap.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().longValue()
        ));
    }
}

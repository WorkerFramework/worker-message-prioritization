/*
 * Copyright 2022-2024 Open Text.
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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.google.common.collect.Sets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageDistributor
{

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDistributor.class);

    public static final String LOAD_BALANCED_INDICATOR = "»";

    private final QueuesApi queuesApi;
    private final boolean targetQueueDurable;
    private final boolean targetQueueExclusive;
    private final boolean targetQueueAutoDelete;
    private final Map<String,Object> targetQueueArgs;

    public MessageDistributor(final QueuesApi queuesApi,
                              final boolean targetQueueDurable,
                              final boolean targetQueueExclusive,
                              final boolean targetQueueAutoDelete,
                              final Map<String,Object> targetQueueArgs)
    {
        this.queuesApi = queuesApi;
        this.targetQueueDurable = targetQueueDurable;
        this.targetQueueExclusive = targetQueueExclusive;
        this.targetQueueAutoDelete = targetQueueAutoDelete;
        this.targetQueueArgs = targetQueueArgs;
    }

    protected Set<DistributorWorkItem> getDistributorWorkItems(final Connection connection) throws IOException
    {
        final List<Queue> queues = queuesApi.getQueues();

        LOGGER.debug("Read the following list of queues from the RabbitMQ API: {}", queues);

        final Set<DistributorWorkItem> distributorWorkItems = new HashSet<>();

        final Set<Queue> targetQueues = getTargetQueues(connection, queues);

        for (final Queue targetQueue : targetQueues) {

            final Set<Queue> stagingQueues = queues.stream()
                    .filter(q ->
                            q.getMessages() > 0 &&
                                    q.getName().startsWith(targetQueue.getName() + LOAD_BALANCED_INDICATOR))
                    .collect(Collectors.toSet());

            if (stagingQueues.isEmpty()) {
                continue;
            }

            LOGGER.debug("Creating a new DistributorWorkItem for target queue: {} and staging queues: {}", targetQueue, stagingQueues);

            distributorWorkItems.add(new DistributorWorkItem(targetQueue, stagingQueues));

        }
        return distributorWorkItems;
    }

    private Set<Queue> getTargetQueues(final Connection connection, final List<Queue> queues) throws IOException
    {
        // The set of target queues that already exist
        final Set<Queue> targetQueuesAlreadyCreated = queues
                .stream()
                .filter(q -> !q.getName().contains(LOAD_BALANCED_INDICATOR))
                .collect(Collectors.toSet());

        // The set of target queues that need to be created (in the scenario where a worker is deployed initially with 0 instances its
        // target queue will not be created, so when this happens we need to create the target queue, otherwise the messages will never
        // get moved on from the staging queues)
        final Set<String> targetQueuesToBeCreated = getTargetQueuesToBeCreated(queues, targetQueuesAlreadyCreated);

        if (!targetQueuesToBeCreated.isEmpty()) {
            LOGGER.info("Target queue(s) to be created: {}", targetQueuesToBeCreated);

            // Create RabbitMQ channel
            try (final Channel channel = connection.createChannel()) {

                // Create target queues
                for (final String targetQueueToBeCreated : targetQueuesToBeCreated) {
                    LOGGER.info(String.format("Creating target queue by calling channel.queueDeclare(%s, %s, %s, %s, %s)",
                            targetQueueToBeCreated, targetQueueDurable, targetQueueExclusive, targetQueueAutoDelete,
                            targetQueueArgs));

                    channel.queueDeclare(
                            targetQueueToBeCreated,
                            targetQueueDurable,
                            targetQueueExclusive,
                            targetQueueAutoDelete,
                            targetQueueArgs);
                }
            } catch (final TimeoutException e) {
                LOGGER.warn("TimeoutException thrown trying to close RabbitMQ channel", e);
            }

            // Return the updated set of target queues
            return queuesApi.getQueues()
                    .stream()
                    .filter(q -> !q.getName().contains(LOAD_BALANCED_INDICATOR))
                    .collect(Collectors.toSet());
        } else {
            // No target queues needed to be created
            return targetQueuesAlreadyCreated;
        }
    }

    private static Set<String> getTargetQueuesToBeCreated(final List<Queue> queues, final Set<Queue> targetQueuesAlreadyCreated)
    {
        // Parse target queues from staging queues
        final Set<String> targetQueuesParsedFromStagingQueues = queues
                .stream()
                .map(Queue::getName)
                .filter(name -> name.contains(LOAD_BALANCED_INDICATOR))
                .map(name -> name.split(LOAD_BALANCED_INDICATOR)[0])
                .collect(Collectors.toSet());

        // Find target queues to be created
        return Sets.difference(targetQueuesParsedFromStagingQueues,
                targetQueuesAlreadyCreated
                        .stream()
                        .map(Queue::getName)
                        .collect(Collectors.toSet()));
    }
}

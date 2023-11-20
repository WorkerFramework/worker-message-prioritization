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
package com.github.workerframework.workermessageprioritization.rerouting;

import static com.github.workerframework.workermessageprioritization.rerouting.MessageRouter.LOAD_BALANCED_INDICATOR;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitQueueConstants;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;

public class StagingQueueCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueCreator.class);
    private final ConnectionFactory connectionFactory;
    private final RabbitManagementApi<QueuesApi> queuesApi;
    private final Supplier<List<String>> memoizedStagingQueueNamesSupplier;
    private Connection connection;
    private Channel channel;

    public StagingQueueCreator(
            final ConnectionFactory connectionFactory,
            final RabbitManagementApi<QueuesApi> queuesApi,
            final long stagingQueueCacheExpiryMilliseconds)
            throws IOException, TimeoutException {
        this.connectionFactory = connectionFactory;
        this.queuesApi = queuesApi;

        this.memoizedStagingQueueNamesSupplier = Suppliers.memoizeWithExpiration(
                this::getStagingQueueNames, stagingQueueCacheExpiryMilliseconds, TimeUnit.MILLISECONDS);

        connectToRabbitMQ();

        LOGGER.debug("Initialised StagingQueueCreator with stagingQueueCacheExpiryMilliseconds={}", stagingQueueCacheExpiryMilliseconds);
    }

    private void connectToRabbitMQ() throws IOException, TimeoutException  {
        try {
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
        } catch (final IOException | TimeoutException e) {
            LOGGER.error("Exception thrown trying to create connection to RabbitMQ", e);

            closeConnectionToRabbitMQ();

            throw e;
        }
    }

    private void reconnectToRabbitMQ() throws IOException, TimeoutException  {
        closeConnectionToRabbitMQ();
        connectToRabbitMQ();
    }

    private void closeConnectionToRabbitMQ()  {
        if (connection != null) {
            try {
                connection.close();
            } catch (final IOException | ShutdownSignalException exception) {
                LOGGER.warn("Exception thrown trying to close RabbitMQ connection", exception);
            }
        }
    }

    void createStagingQueue(final Queue targetQueue, final String stagingQueueName) throws IOException {

        final List<String> stagingQueues = memoizedStagingQueueNamesSupplier.get();

        if (stagingQueues.contains(stagingQueueName)) {
            LOGGER.debug("A staging queue named {} already exists in the staging queue cache, so not creating it.",
                    stagingQueueName);

            return;
        }

        final boolean durable = targetQueue.isDurable();
        final boolean exclusive = targetQueue.isExclusive();
        final boolean autoDelete = targetQueue.isAuto_delete();
        Map<String, Object> arguments = new HashMap<>();
        if (Objects.equals(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_NAME, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM)) {
            arguments.put(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_NAME);
        } else {
            arguments = targetQueue.getArguments();
        }

        LOGGER.info("A staging queue named {} does NOT exist in the staging queue cache, " +
                        "so creating or checking staging queue by calling channel.queueDeclare({}, {}, {}, {}, {})",
                stagingQueueName, stagingQueueName, durable, exclusive, autoDelete, arguments);

        try {
            channel.queueDeclare(stagingQueueName, durable, exclusive, autoDelete, arguments);
        } catch (final IOException ioException) {
            LOGGER.error(String.format(
                    "IOException thrown creating or checking staging queue when calling channel.queueDeclare(%s, %s, %s, %s, %s)",
                    stagingQueueName, durable, exclusive, autoDelete, arguments), ioException);

            try {
                reconnectToRabbitMQ();
            } catch (final IOException | TimeoutException ignored) {
                // An error will already be logged if we're unable to reconnect, no need to log it again
            }

            // Throw original exception
            throw ioException;
        }
    }

    private List<String> getStagingQueueNames() {
        return queuesApi.getApi()
                .getQueues("name")
                .stream()
                .map(Queue::getName)
                .filter(name -> name.contains(LOAD_BALANCED_INDICATOR))
                .collect(Collectors.toList());
    }
}

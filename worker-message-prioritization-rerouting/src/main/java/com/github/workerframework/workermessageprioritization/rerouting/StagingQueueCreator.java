/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingQueueCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueCreator.class);

    // We want a single-element (list of all staging queues) LoadingCache, in order to reduce the number of network requests to
    // the RabbitMQ API, but LoadingCache doesn't allow null keys, so we use a dummy key instead.
    //
    // See: https://github.com/google/guava/issues/872
    private static final Object DUMMY_CACHE_KEY = new Object();

    private final ConnectionFactory connectionFactory;
    private final LoadingCache<Object,List<String>> existingStagingQueueNamesCache;
    private Connection connection;
    private Channel channel;

    public StagingQueueCreator(final ConnectionFactory connectionFactory, final RabbitManagementApi<QueuesApi> queuesApi)
            throws IOException, TimeoutException {
        this.connectionFactory = connectionFactory;

        this.existingStagingQueueNamesCache =  CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<Object,List<String>>() {
                    @Override
                    public List<String> load(@Nonnull final Object ignoredKey) {

                        return queuesApi.getApi()
                                .getQueues("name")
                                .stream()
                                .map(Queue::getName)
                                .filter(name -> name.contains(LOAD_BALANCED_INDICATOR))
                                .collect(Collectors.toList());
                    }
                });

        connectToRabbitMQ();
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

        try {
            final List<String> existingStagingQueueNames = existingStagingQueueNamesCache.get(DUMMY_CACHE_KEY);

            if (existingStagingQueueNames.contains(stagingQueueName)) {
                LOGGER.debug("A staging queue named {} already exists in the existingStagingQueueNamesCache, so not creating it.",
                        stagingQueueName);

                return;
            };
        } catch (final ExecutionException e) {
            LOGGER.warn(String.format("ExecutionException thrown trying to check if a staging queue named %s already exists before " +
                    "creating it, so assuming that it does not exist.", stagingQueueName), e);
        }

        final boolean durable = targetQueue.isDurable();
        final boolean exclusive = targetQueue.isExclusive();
        final boolean autoDelete = targetQueue.isAuto_delete();
        final Map<String, Object> arguments = targetQueue.getArguments();

        LOGGER.info("A staging queue named {} does NOT exist in the existingStagingQueueNamesCache," +
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

        channel.basicConsume(stagingQueueName, new HandleCancelConsumer(channel, stagingQueueName));

        existingStagingQueueNamesCache.refresh(DUMMY_CACHE_KEY);
    }

    final class HandleCancelConsumer extends DefaultConsumer
    {
        private final String queueName;

        public HandleCancelConsumer(final Channel channel, final String queueName)
        {
            super(channel);
            this.queueName = queueName;
        }

        @Override
        public void handleCancel(String consumerTag) throws IOException
        {
            LOGGER.warn("HandleCancelConsumer.handleCancel was called for a queue named {}. " +
                            "This means the queue may have been deleted. Will now refresh the existingStagingQueueNamesCache.",
                    queueName);

            existingStagingQueueNamesCache.refresh(DUMMY_CACHE_KEY);
        }
    }
}

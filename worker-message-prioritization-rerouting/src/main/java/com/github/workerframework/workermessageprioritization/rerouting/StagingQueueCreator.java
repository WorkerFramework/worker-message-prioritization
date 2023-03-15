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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingQueueCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueCreator.class);

    private final HashSet<String> declaredQueues = new HashSet<>();
    private final ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;

    public StagingQueueCreator(final ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        this.connectionFactory = connectionFactory;
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

        if(declaredQueues.contains(stagingQueueName)) {
            return;
        }

        final boolean durable = targetQueue.isDurable();
        final boolean exclusive = targetQueue.isExclusive();
        final boolean autoDelete = targetQueue.isAuto_delete();
        final Map<String, Object> arguments = targetQueue.getArguments();

        LOGGER.info("Creating or checking staging queue by calling channel.queueDeclare({}, {}, {}, {}, {})",
                stagingQueueName, durable, exclusive, autoDelete, arguments);

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

        declaredQueues.add(stagingQueueName);
    }
}

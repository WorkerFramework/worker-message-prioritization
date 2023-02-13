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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingQueueCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StagingQueueCreator.class);

    private final HashSet<String> declaredQueues = new HashSet<>();
    private final Channel channel;

    public StagingQueueCreator(final Channel channel) {

        this.channel = channel;
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
        } catch (final IOException iOException) {
            LOGGER.error(String.format(
                    "IOException thrown creating or checking staging queue when calling channel.queueDeclare(%s, %s, %s, %s, %s)",
                    stagingQueueName, durable, exclusive, autoDelete, arguments), iOException);

            throw iOException;
        }

        declaredQueues.add(stagingQueueName);
    }
}

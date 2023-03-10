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
import com.rabbitmq.client.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class StagingQueueCreatorIT extends RerouterTestBase {

    @Test
    public void createStagingQueueTest() throws TimeoutException, IOException, InterruptedException {

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueueName = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);

        try (final Connection connection = connectionFactory.newConnection()) {

            try (final Channel channel = connection.createChannel()) {

                // Create a target queue
                final boolean targetQueueDurable = true;
                final boolean targetQueueExclusive = false;
                final boolean targetQueueAutoDelete = false;
                final Map<String, Object> targetQueueArguments = Collections.singletonMap("x-max-priority", 5L);
                channel.queueDeclare(
                    targetQueueName, targetQueueDurable, targetQueueExclusive, targetQueueAutoDelete, targetQueueArguments);

                // Verify the target queue was created successfully
                final Queue targetQueue = queuesApi.getApi().getQueue("/", targetQueueName);
                Assert.assertNotNull("Target queue was not found via REST API", targetQueue);

                // Create a staging queue using the target queue as a template
                final StagingQueueCreator stagingQueueCreator = new StagingQueueCreator(connectionFactory);
                stagingQueueCreator.createStagingQueue(targetQueue, stagingQueueName);

                // Verify the staging queue was created successfully
                final Queue stagingQueue = queuesApi.getApi().getQueue("/", stagingQueueName);
                Assert.assertNotNull("Staging queue was not found via REST API", stagingQueue);
                Assert.assertEquals("Staging queue should have been created with the supplied name",
                                    stagingQueueName, stagingQueue.getName());
                Assert.assertEquals("Staging queue should have been created with the same durable setting as the target queue",
                                    targetQueueDurable, stagingQueue.isDurable());
                Assert.assertEquals("Staging queue should have been created with the same exclusive setting as the target queue",
                                    targetQueueExclusive, stagingQueue.isExclusive());
                Assert.assertEquals("Staging queue should have been created with the same auto_delete setting as the target queue",
                                    targetQueueAutoDelete, stagingQueue.isAuto_delete());
                Assert.assertEquals("Staging queue should have been created with the same arguments as the target queue",
                                    targetQueueArguments, stagingQueue.getArguments());
            }
        }
    }
}

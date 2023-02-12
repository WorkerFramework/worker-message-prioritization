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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

// This test will used whatever distributor implementation (low level or shovel) has been set as the mainClass in the maven-jar-plugin in
// worker-message-prioritization-distribution/pom.xml.
public final class DistributorIT extends DistributorTestBase {

    @Test
    public void twoStagingQueuesTest() throws TimeoutException, IOException, InterruptedException {

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue1Name = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);
        final String stagingQueue2Name = getStagingQueueName(targetQueueName, T2_STAGING_QUEUE_NAME);
        
        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(stagingQueue2Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(targetQueueName, true, false, false, Collections.emptyMap());

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final String body = gson.toJson(new Object());

            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));
        }

        Queue targetQueue = null;
        try(final Connection connection = connectionFactory.newConnection()) {
            for(int attempt = 0; attempt < 10; attempt ++) {
                targetQueue = queuesApi.getApi().getQueue("/", targetQueueName);
                if(targetQueue.getMessages() > 0) {
                    break;
                }

                Thread.sleep(1000 * 10);
            }
        }

        Assert.assertNotNull("Target queue was not found via REST API", targetQueue);
        final Queue stagingQueue1 = queuesApi.getApi().getQueue("/", stagingQueue1Name);
        final Queue stagingQueue2 = queuesApi.getApi().getQueue("/", stagingQueue2Name);
        Assert.assertEquals("Two staged messages should be on target queue", 2L, targetQueue.getMessages());
        Assert.assertEquals("1st Staging queue should be empty", 0L, stagingQueue1.getMessages());
        Assert.assertEquals("2n Staging queue should be empty", 0L, stagingQueue2.getMessages());
    }
}

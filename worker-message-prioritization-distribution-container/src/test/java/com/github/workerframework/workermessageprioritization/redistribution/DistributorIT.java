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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitQueueConstants;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

// This test will use whatever distributor implementation (low level) has been set as the mainClass in the maven-jar-plugin in
// worker-message-prioritization-distribution/pom.xml.
public final class DistributorIT extends DistributorTestBase {

    @Test
    public void twoStagingQueuesTest() throws TimeoutException, IOException, InterruptedException {

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue1Name = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);
        final String stagingQueue2Name = getStagingQueueName(targetQueueName, T2_STAGING_QUEUE_NAME);
        
        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            final Map<String, Object> args = new HashMap<>();
            args.put(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM);

            channel.queueDeclare(stagingQueue1Name, true, false, false, args);
            channel.queueDeclare(stagingQueue2Name, true, false, false, args);
            channel.queueDeclare(targetQueueName, true, false, false, args);

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            final String body = "{}";

            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));
        }

        await().alias(String.format("Waiting for target queue named %s to contain 2 message", targetQueueName))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(queueContainsNumMessages(targetQueueName, 2));

        await().alias(String.format("Waiting for 1st staging queue named %s to contain 0 messages", stagingQueue1Name))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(queueContainsNumMessages(stagingQueue1Name, 0));

        await().alias(String.format("Waiting for 2nd staging queue named %s to contain 0 messages", stagingQueue2Name))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(queueContainsNumMessages(stagingQueue2Name, 0));
    }

    @Test
    public void targetQueuesAreCreatedIfTheyDoNotExistTest() throws TimeoutException, IOException, InterruptedException
    {
        final String targetQueue1Name = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue1Name = getStagingQueueName(targetQueue1Name, T1_STAGING_QUEUE_NAME);
        final String stagingQueue2Name = getStagingQueueName(targetQueue1Name, T2_STAGING_QUEUE_NAME);

        final String targetQueue2Name = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue3Name = getStagingQueueName(targetQueue2Name, T1_STAGING_QUEUE_NAME);
        final String stagingQueue4Name = getStagingQueueName(targetQueue2Name, T2_STAGING_QUEUE_NAME);

        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            final Map<String, Object> args = new HashMap<>();
            args.put(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM);

            // Note the 2 target queues are not created, they should be created automatically
            channel.queueDeclare(stagingQueue1Name, true, false, false, args);
            channel.queueDeclare(stagingQueue2Name, true, false, false, args);
            channel.queueDeclare(stagingQueue3Name, true, false, false, args);
            channel.queueDeclare(stagingQueue4Name, true, false, false, args);

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            final String body = "{}";

            // Publish 1 message to each staging queue
            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue3Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue4Name, properties, body.getBytes(StandardCharsets.UTF_8));

            await().alias(String.format("Waiting for 1st target queue named %s to be created", targetQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueExists(targetQueue1Name));

            await().alias(String.format("Waiting for 1st target queue named %s to contain 2 messages", targetQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueue1Name, 2));

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 0 messages", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 0));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 0 messages", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 0));

            await().alias(String.format("Waiting for 2nd target queue named %s to be created", targetQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueExists(targetQueue2Name));

            await().alias(String.format("Waiting for 2nd target queue named %s to contain 2 messages", targetQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueue2Name, 2));

            await().alias(String.format("Waiting for 3rd staging queue named %s to contain 0 messages", stagingQueue3Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue3Name, 0));

            await().alias(String.format("Waiting for 4th staging queue named %s to contain 0 messages", stagingQueue4Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue4Name, 0));
        }
    }
}

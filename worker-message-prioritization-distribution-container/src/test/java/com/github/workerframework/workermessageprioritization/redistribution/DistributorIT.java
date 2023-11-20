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
package com.github.workerframework.workermessageprioritization.redistribution;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

// This test will use whatever distributor implementation (low level) has been set as the mainClass in the maven-jar-plugin in
// worker-message-prioritization-distribution/pom.xml.
public final class DistributorIT extends DistributorTestBase {

    private static final String RABBIT_PROP_QUEUE_TYPE = "x-queue-type";
    private static final String RABBIT_PROP_QUEUE_TYPE_CLASSIC = "classic";
    private static final String RABBIT_PROP_QUEUE_TYPE_QUORUM = "quorum";
    private static final String RABBIT_PROP_QUEUE_TYPE_NAME = !Strings.isNullOrEmpty(System.getenv("RABBIT_PROP_QUEUE_TYPE_NAME"))?
            System.getenv("RABBIT_PROP_QUEUE_TYPE_NAME") : RABBIT_PROP_QUEUE_TYPE_CLASSIC;

    @Test
    public void twoStagingQueuesTest() throws TimeoutException, IOException, InterruptedException {

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue1Name = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);
        final String stagingQueue2Name = getStagingQueueName(targetQueueName, T2_STAGING_QUEUE_NAME);
        
        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            final Map<String, Object> args = new HashMap<>();
            args.put(RABBIT_PROP_QUEUE_TYPE, RABBIT_PROP_QUEUE_TYPE_NAME);

            channel.queueDeclare(stagingQueue1Name, true, false, false, args);
            channel.queueDeclare(stagingQueue2Name, true, false, false, args);
            channel.queueDeclare(targetQueueName, true, false, false, args);

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            final String body = gson.toJson(new Object());

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
}

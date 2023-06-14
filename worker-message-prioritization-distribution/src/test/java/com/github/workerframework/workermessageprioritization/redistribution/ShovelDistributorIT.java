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

import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.shovel.ShovelDistributor;
import com.github.workerframework.workermessageprioritization.targetqueue.FixedTargetQueueSettingsProvider;
import com.rabbitmq.client.AMQP;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class ShovelDistributorIT extends DistributorTestBase {

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

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 1 message", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 1));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 1 message", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 1));
        }

        final ConsumptionTargetCalculator consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(new FixedTargetQueueSettingsProvider());

        final ShovelDistributor shovelDistributor = new ShovelDistributor(
                queuesApi,
                shovelsApi,
                nodesApi,
                getNodeSpecificShovelsApiCache(),
                consumptionTargetCalculator,
                System.getProperty("rabbitmq.username", "guest"),
                "/",
                120000,
                120000,
                1800000,
                120000,
                120000,
                120000,
                10000);

        shovelDistributor.runOnce();

        await().alias(String.format("Waiting for target queue named %s to contain 2 messages", targetQueueName))
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

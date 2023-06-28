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
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.LowLevelDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributorIT extends DistributorTestBase {

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

            // Publish 500 messages to each staging queue, 1000 in total (note using large numbers of messages here as some issues only
            // present themselves when there are large numbers of messages)
            for (int i = 0; i < 500; i++) {
                channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
                channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));
            }

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 500 messages", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 500));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 500 messages", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 500));

            // Target queue has a max length of 200 messages
            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(targetQueue -> new TargetQueueSettings(200, 0));
            final StagingTargetPairProvider stagingTargetPairProvider = new StagingTargetPairProvider();
            final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(queuesApi, connectionFactory,
                    consumptionTargetCalculator, stagingTargetPairProvider, 10000, 1800000);

            // Run the distributor (1st time).
            // stagingQueue1:            500 messages
            // stagingQueue2:            500 messages
            // targetQueue:              0 messages
            // targetQueueMaxLength:     200
            // Expected result:          100 messages from each staging queue moved to target queue
            Assert.assertEquals(
                    "Expected 0 StagingQueueTargetQueuePairs before running the distributor for the 1st time",
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size());

            lowLevelDistributor.runOnce(connection);

            Assert.assertEquals(
                    "Expected 2 StagingQueueTargetQueuePairs to be created after running the distributor for the 1st time",
                    2,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size());

            await().alias(String.format("Waiting for target queue named %s to contain 100 messages", targetQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueueName, 200));

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 450 messages", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 400));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 450 messages", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 400));

            // Run the distributor (2nd time).
            // stagingQueue1:            400 messages
            // stagingQueue2:            400 messages
            // targetQueue:              200 messages
            // targetQueueMaxLength:     200
            // Expected result:          0 messages from each staging queue moved to target queue because target queue is full
            lowLevelDistributor.runOnce(connection);

            Assert.assertEquals(
                    "Expected 0 StagingQueueTargetQueuePairs after running the distributor for the 2nd time (because the " +
                            "initial 2 StagingQueueTargetQueuePairs should have been closed and removed as they have completed, " +
                            "and no more StagingQueueTargetQueuePairs should have been created because the target queue is full)",
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size());

            await().alias(String.format("Waiting for target queue named %s to contain 200 messages", targetQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueueName, 200));

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 400 messages", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 400));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 400 messages", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 400));

            // Run the distributor (3rd time).
            // stagingQueue1:            400 messages
            // stagingQueue2:            400 messages
            // targetQueue:              0 messages (we have purged it)
            // targetQueueMaxLength:     200
            // Expected result:          100 messages from each staging queue moved to target queue
            channel.queuePurge(targetQueueName);

            await().alias(String.format("Waiting for target queue named %s to contain 0 messages", targetQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueueName, 0));

            lowLevelDistributor.runOnce(connection);

            Assert.assertEquals(
                    "Expected 2 StagingQueueTargetQueuePairs to be created after running the distributor for the 3rd time (because the " +
                            "target queue has been purged and so now has capacity for more messages)",
                    2,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size());

            await().alias(String.format("Waiting for target queue named %s to contain 200 messages", targetQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(targetQueueName, 200));

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 300 messages", stagingQueue1Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue1Name, 300));

            await().alias(String.format("Waiting for 2nd staging queue named %s to contain 300 messages", stagingQueue2Name))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueue2Name, 300));
        }
    }
}

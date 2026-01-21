/*
 * Copyright 2022-2026 Open Text.
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
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.LowLevelDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingQueueTargetQueuePair;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.mockito.Mockito;
import org.mockito.internal.util.collections.Sets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class LowLevelDistributorIT extends DistributorTestBase {

    @Test
    public void twoStagingQueuesTest() throws TimeoutException, IOException {

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

            final Injector injector = Guice.createInjector(new DistributorModule());
            final CapacityCalculatorBase capacityCalculatorBase = injector.getInstance(CapacityCalculatorBase.class);
            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(targetQueue -> new TargetQueueSettings(200, 0, 1, 1, 200),
                            capacityCalculatorBase);
            final StagingTargetPairProvider stagingTargetPairProvider = injector.getInstance(StagingTargetPairProvider.class);
            final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(queuesApi, connectionFactory,
                    consumptionTargetCalculator, stagingTargetPairProvider, 10000, 600000, true, false, false,
                    Map.of("x-queue-type", "quorum"));

            // Run the distributor (1st time).
            // stagingQueue1:            500 messages
            // stagingQueue2:            500 messages
            // targetQueue:              0 messages
            // targetQueueMaxLength:     200
            // Expected result:          100 messages from each staging queue moved to target queue
            assertEquals(
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected 0 StagingQueueTargetQueuePairs before running the distributor for the 1st time");

            lowLevelDistributor.runOnce(connection);

            assertEquals(
                    2,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected 2 StagingQueueTargetQueuePairs to be created after running the distributor for the 1st time");

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

            assertEquals(
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected 0 StagingQueueTargetQueuePairs after running the distributor for the 2nd time (because the " +
                            "initial 2 StagingQueueTargetQueuePairs should have been closed and removed as they have completed, " +
                            "and no more StagingQueueTargetQueuePairs should have been created because the target queue is full)");

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

            assertEquals(
                    2,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                     "Expected 2 StagingQueueTargetQueuePairs to be created after running the distributor for the 3rd time (because the " +
                            "target queue has been purged and so now has capacity for more messages)");

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

    @Test
    public void stagingQueueTargetQueuePairIsClosedWhenLastDoneWorkTimeoutExceededTest()
            throws TimeoutException, IOException, InterruptedException
    {
        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueueName = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);

        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            final Map<String, Object> args = new HashMap<>();
            args.put(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM);

            channel.queueDeclare(stagingQueueName, true, false, false, args);
            channel.queueDeclare(targetQueueName, true, false, false, args);

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .build();

            final String body = "{}";

            // Publish 1 message to the staging queue
            channel.basicPublish("", stagingQueueName, properties, body.getBytes(StandardCharsets.UTF_8));

            await().alias(String.format("Waiting for 1st staging queue named %s to contain 1 message", stagingQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages(stagingQueueName, 1));

            // Mocking consumptionLimit so that is more (2) than the number of messages in the staging queue (1) to keep the
            // StagingQueueTargetQueuePair running without doing any work after the first message is consumed
            final Queue stagingQueue = new Queue();
            stagingQueue.setName(stagingQueueName);
            stagingQueue.setMessages(1);
            stagingQueue.setMessages_ready(1);
            stagingQueue.setDurable(true);
            stagingQueue.setExclusive(false);
            stagingQueue.setAuto_delete(false);
            stagingQueue.setArguments(Collections.emptyMap());

            final Queue targetQueue = new Queue();
            targetQueue.setName(targetQueueName);
            targetQueue.setMessages(0);
            targetQueue.setMessages_ready(0);
            targetQueue.setDurable(true);
            targetQueue.setExclusive(false);
            targetQueue.setAuto_delete(false);
            targetQueue.setArguments(Collections.emptyMap());

            final long consumerPublisherPairLastDoneWorkTimeoutMilliseconds = 1000;

            final StagingQueueTargetQueuePair stagingQueueTargetQueuePair = new StagingQueueTargetQueuePair(
                    connection,
                    stagingQueue,
                    targetQueue,
                    2,
                    consumerPublisherPairLastDoneWorkTimeoutMilliseconds
            );

            final Injector injector = Guice.createInjector(new DistributorModule());
            final CapacityCalculatorBase capacityCalculatorBase = injector.getInstance(CapacityCalculatorBase.class);

            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(tQ -> new TargetQueueSettings(2, 0,
                            1, 1, 2), capacityCalculatorBase);

            final StagingTargetPairProvider stagingTargetPairProviderMock = Mockito.mock(StagingTargetPairProvider.class);

            Mockito.when(stagingTargetPairProviderMock.provideStagingTargetPairs(
                            Mockito.any(Connection.class),
                            Mockito.any(DistributorWorkItem.class),
                            Mockito.any(Map.class),
                            Mockito.anyLong()))
                    .thenReturn(Sets.newSet(stagingQueueTargetQueuePair))
                    .thenAnswer(invocation -> Sets.newSet()); // Prevent new StagingTargetPairProvider from being created

            final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(queuesApi, connectionFactory,
                    consumptionTargetCalculator, stagingTargetPairProviderMock, 10000,
                    consumerPublisherPairLastDoneWorkTimeoutMilliseconds, true, false, false,
                    Map.of(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM));

            // Run the distributor (1st time)
            assertEquals(
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected 0 StagingQueueTargetQueuePairs before running the distributor for the 1st time");

            lowLevelDistributor.runOnce(connection);

            assertEquals(
                    1,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected 1 StagingQueueTargetQueuePair to be created after running the distributor for the 1st time");

            // Wait for more than the consumerPublisherPairLastWorkTimeoutMilliseconds
            Thread.sleep(consumerPublisherPairLastDoneWorkTimeoutMilliseconds + 5000);

            // Run the distributor (2nd time)
            lowLevelDistributor.runOnce(connection);

            assertEquals(
                    0,
                    lowLevelDistributor.getExistingStagingQueueTargetQueuePairs().size(),
                    "Expected the StagingQueueTargetQueuePair to be closed and removed after the " +
                            "consumerPublisherPairLastWorkTimeoutMilliseconds has been exceeded but it was not: "
                            + lowLevelDistributor.getExistingStagingQueueTargetQueuePairs());
        }
    }
}

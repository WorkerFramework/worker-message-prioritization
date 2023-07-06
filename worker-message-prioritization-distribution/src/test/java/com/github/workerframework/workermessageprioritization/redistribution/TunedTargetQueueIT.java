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

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.LowLevelDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TunedTargetQueueIT extends DistributorTestBase {

    final String queueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
    final String stagingQueue1Name = getStagingQueueName(queueName, T1_STAGING_QUEUE_NAME);
    final String stagingQueue2Name = getStagingQueueName(queueName, T2_STAGING_QUEUE_NAME);

    // This test is for development purposes only
    // This test is to observe the consumption rate altering the recommended target queue length.
    // Console outputs and debugger should be used to see these dynamic changes.
    @Test
    public void tunedTargetQueueNoOpModeTest() throws TimeoutException, IOException {
        try (final Connection connection = connectionFactory.newConnection()) {

            final int queueSize = 500000;
            final int sleepTime = 10000;
            final int roundingMultiple = 100;
            final int maxConsumptionRateHistorySize = 100;
            final int minConsumptionRateHistorySize = 100;
            final long currentTargetQueueLength = 1000;
            final long eligibleForRefillPercent = 10;
            final double maxInstances = 4;
            final double currentInstances = 2;
            final int queueProcessingTimeGoalSeconds = 300;

            final boolean noOpMode = !Strings.isNullOrEmpty(System.getenv("CAF_NOOP_MODE")) ?
                    Boolean.parseBoolean(System.getenv("CAF_NOOP_MODE")) : true;

            Channel channel = connection.createChannel();

            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(stagingQueue2Name, true, false, false, Collections.emptyMap());

            Assert.assertNotNull("Queue was not found via REST API", queuesApi.getApi().getQueue("/", queueName));
            Assert.assertNotNull("Staging queue was not found via REST API", queuesApi.getApi().getQueue("/", stagingQueue1Name));
            Assert.assertNotNull("Staging queue was not found via REST API", queuesApi.getApi().getQueue("/", stagingQueue2Name));

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final String body = gson.toJson(new Object());

            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));

            // Verify the target queue was created successfully
            final Queue targetQueue = queuesApi.getApi().getQueue("/", queueName);
            Assert.assertNotNull("Target queue was not found via REST API", targetQueue);

            String message = "Hello World!";
            IntStream.range(1, queueSize).forEach(i -> {
                try {
                    channel.basicPublish("", queueName, null, message.getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                new String(delivery.getBody(), "UTF-8");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

            // Sleep to allow time for the messages to start being consumed.
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = new TargetQueuePerformanceMetricsProvider(queuesApi);
            final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maxConsumptionRateHistorySize, minConsumptionRateHistorySize);
            final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(roundingMultiple);
            final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(currentTargetQueueLength, eligibleForRefillPercent,
                    maxInstances, currentInstances);

            final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = new TunedTargetQueueLengthProvider(
                    targetQueuePerformanceMetricsProvider,
                    historicalConsumptionRate,
                    roundTargetQueueLength,
                    noOpMode,
                    queueProcessingTimeGoalSeconds);

            final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
            when(targetQueueSettingsProvider.get(argThat(new QueueNameMatcher(queueName)))).thenReturn(targetQueueSettings);

            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, tunedTargetQueueLengthProvider);
            final StagingTargetPairProvider stagingTargetPairProvider = new StagingTargetPairProvider();

            final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(
                    queuesApi,
                    connectionFactory,
                    consumptionTargetCalculator,
                    stagingTargetPairProvider,
                    10000,
                    600000);

            lowLevelDistributor.runOnce(connection);

        }
    }

    private static class QueueNameMatcher implements ArgumentMatcher<Queue> {

        private final String queueName;

        public QueueNameMatcher(final String queueName) {

            this.queueName = queueName;
        }

        @Override
        public boolean matches(final Queue queue) {
            return queueName.equals(queue.getName());
        }
    }
}

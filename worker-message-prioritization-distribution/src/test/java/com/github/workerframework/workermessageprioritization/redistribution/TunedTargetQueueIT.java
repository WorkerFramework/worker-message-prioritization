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
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.QueueConsumptionRateProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.internal.MockWebServerExtension;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

@ExtendWith(MockWebServerExtension.class)
public class TunedTargetQueueIT extends DistributorTestBase {

    final String queueName = "elastic-query-worker";
    final String stagingQueue1Name = getStagingQueueName(queueName, T1_STAGING_QUEUE_NAME);
    final String stagingQueue2Name = getStagingQueueName(queueName, T2_STAGING_QUEUE_NAME);
    public static final String MOCK_SERVER_PORT = "CAF_MOCK_SERVER_PORT";
    private static final String CAF_ROUNDING_MULTIPLE = "CAF_ROUNDING_MULTIPLE";
    private static final String CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE = "CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE";
    private static final String CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE = "CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE";
    private static final String CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS = "CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS";
    private static final String CAF_MIN_TARGET_QUEUE_LENGTH = "CAF_MIN_TARGET_QUEUE_LENGTH";
    private static final String CAF_MAX_TARGET_QUEUE_LENGTH = "CAF_MAX_TARGET_QUEUE_LENGTH";

    // This test is for development purposes only
    // This test is to observe the consumption rate altering the recommended target queue length.
    // Console outputs and debugger should be used to see these dynamic changes.
//    @Test
    public void tunedTargetQueueNoOpModeTest() throws TimeoutException, IOException {
        try (final Connection connection = connectionFactory.newConnection()) {

            try (final MockWebServer mockK8sServer = new MockWebServer()) {
                final int queueSize = 500000;
                final int sleepTime = 100;
                final int roundingMultiple = Integer.parseInt(System.getenv(CAF_ROUNDING_MULTIPLE));
                final int maxConsumptionRateHistorySize = Integer.parseInt(System.getenv(CAF_MAX_CONSUMPTION_RATE_HISTORY_SIZE));
                final int minConsumptionRateHistorySize = Integer.parseInt(System.getenv(CAF_MIN_CONSUMPTION_RATE_HISTORY_SIZE));
                final int queueProcessingTimeGoalSeconds = Integer.parseInt(System.getenv(CAF_QUEUE_PROCESSING_TIME_GOAL_SECONDS));
                final int minTargetQueueLength = Integer.parseInt(System.getenv(CAF_MIN_TARGET_QUEUE_LENGTH));
                final int maxTargetQueueLength = Integer.parseInt(System.getenv(CAF_MAX_TARGET_QUEUE_LENGTH));

                final boolean noOpMode = Strings.isNullOrEmpty(System.getenv("CAF_NOOP_MODE")) || Boolean.parseBoolean(System.getenv("CAF_NOOP_MODE"));

                Channel channel = connection.createChannel();

                channel.queueDeclare(queueName, false, false, false, null);
                channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
                channel.queueDeclare(stagingQueue2Name, true, false, false, Collections.emptyMap());

                Assertions.assertNotNull(queuesApi.getApi().getQueue("/", queueName), "Queue was not found via REST API");
                Assertions.assertNotNull(queuesApi.getApi().getQueue("/", stagingQueue1Name), "Staging queue was not found via REST API");
                Assertions.assertNotNull(queuesApi.getApi().getQueue("/", stagingQueue2Name), "Staging queue was not found via REST API");

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
                Assertions.assertNotNull(targetQueue, "Target queue was not found via REST API");

                String message = "Hello World!";
                IntStream.range(1, queueSize).forEach(i -> {
                    try {
                        channel.basicPublish("", queueName, null, message.getBytes());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    new String(delivery.getBody(), StandardCharsets.UTF_8);
                };
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                });

                // Sleep to allow time for the messages to start being consumed.
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                final String deploymentList = IOUtils.resourceToString("k8s/deploymentList.json", StandardCharsets.UTF_8,
                        getClass().getClassLoader());
                final String apiVersions = IOUtils.resourceToString("k8s/apiVersions.json", StandardCharsets.UTF_8,
                        getClass().getClassLoader());
                final String apiResourceList = IOUtils.resourceToString("k8s/apiResourceList.json", StandardCharsets.UTF_8,
                        getClass().getClassLoader());
                final String apiGroupList = IOUtils.resourceToString("k8s/apiGroupList.json", StandardCharsets.UTF_8,
                        getClass().getClassLoader());

                mockK8sServer.start(InetAddress.getByName("localhost"), Integer.parseInt(System.getenv(MOCK_SERVER_PORT)));

                mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(apiVersions));

                mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(apiResourceList));

                mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(apiGroupList));

                IntStream.range(1, 21).forEach(i -> mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(apiResourceList)));

                mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(deploymentList));

                mockK8sServer.enqueue(new MockResponse()
                        .setResponseCode(200)
                        .setHeader("content-type", "application/json")
                        .setBody(deploymentList));

                QueueConsumptionRateProvider queueConsumptionRateProvider = new QueueConsumptionRateProvider(queuesApi);
                final HistoricalConsumptionRateManager historicalConsumptionRateManager = new HistoricalConsumptionRateManager(maxConsumptionRateHistorySize, minConsumptionRateHistorySize);
                final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(roundingMultiple);

                final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = new TunedTargetQueueLengthProvider(
                        queueConsumptionRateProvider,
                        historicalConsumptionRateManager,
                        targetQueueLengthRounder,
                        minTargetQueueLength,
                        maxTargetQueueLength,
                        noOpMode,
                        queueProcessingTimeGoalSeconds);

                List<String> namespaces = new ArrayList<>();
                namespaces.add("private");

                final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider = new K8sTargetQueueSettingsProvider(
                        namespaces,
                        60);

                final ConsumptionTargetCalculator consumptionTargetCalculator =
                        new EqualConsumptionTargetCalculator(k8sTargetQueueSettingsProvider, null);
                final StagingTargetPairProvider stagingTargetPairProvider = new StagingTargetPairProvider();

                final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(
                        queuesApi,
                        connectionFactory,
                        consumptionTargetCalculator,
                        stagingTargetPairProvider,
                        10000,
                        600000,
                        100,
                        10000000);

                lowLevelDistributor.runOnce(connection);

                mockK8sServer.shutdown();
            }
        }
    }
}

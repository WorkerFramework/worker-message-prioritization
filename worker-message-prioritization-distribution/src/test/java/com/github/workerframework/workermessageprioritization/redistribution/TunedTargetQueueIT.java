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
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitQueueConstants;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.LowLevelDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.internal.MockWebServerExtension;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

@ExtendWith(MockWebServerExtension.class)
public class TunedTargetQueueIT extends DistributorTestBase {
    public static final String MOCK_SERVER_PORT = "CAF_MOCK_SERVER_PORT";
    final String queueName = "elastic-query-worker";
    final String stagingQueue1Name = getStagingQueueName(queueName, T1_STAGING_QUEUE_NAME);
    final String stagingQueue2Name = getStagingQueueName(queueName, T2_STAGING_QUEUE_NAME);

    // This test is for development purposes only
    // This test is to observe the consumption rate altering the recommended target queue length.
    // Console outputs and debugger should be used to see these dynamic changes.
//    @Test
    public void tunedTargetQueueTuningDisabledTest() throws TimeoutException, IOException {

        final Logger LOGGER = LoggerFactory.getLogger(TunedTargetQueueIT.class);

        try (final Connection connection = connectionFactory.newConnection()) {

            try (final MockWebServer mockK8sServer = new MockWebServer()) {
                final int queueSize = 500000;
                final int sleepTime = 100;

                Channel channel = connection.createChannel();

                final Map<String, Object> args = new HashMap<>();
                args.put(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM);

                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueDeclare(stagingQueue1Name, true, false, false, args);
                channel.queueDeclare(stagingQueue2Name, true, false, false, args);

                Assertions.assertNotNull(queuesApi.getQueue("/", queueName), "Queue was not found via REST API");
                Assertions.assertNotNull(queuesApi.getQueue("/", stagingQueue1Name), "Staging queue was not found via REST API");
                Assertions.assertNotNull(queuesApi.getQueue("/", stagingQueue2Name), "Staging queue was not found via REST API");

                final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/json")
                        .deliveryMode(2)
                        .build();

                final String body = "{}";

                channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
                channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));

                // Verify the target queue was created successfully
                final Queue targetQueue = queuesApi.getQueue("/", queueName);
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
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    final String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    LOGGER.debug("Message body: " + messageBody);
                };
                channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
                    LOGGER.debug("Consumer cancelled");
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

                mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(apiVersions)
                        .build());

                mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(apiResourceList)
                        .build());

                mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(apiGroupList)
                        .build());

                IntStream.range(1, 21).forEach(i -> mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(apiResourceList)
                        .build()));

                mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(deploymentList)
                        .build());

                mockK8sServer.enqueue(new MockResponse.Builder()
                        .code(200)
                        .setHeader("content-type", "application/json")
                        .body(deploymentList)
                        .build());

                final Injector injector = Guice.createInjector(new DistributorModule());
                final TargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                        injector.getInstance(TargetQueueSettingsProvider.class);

                final CapacityCalculatorBase capacityCalculatorBase = injector.getInstance(CapacityCalculatorBase.class);

                final ConsumptionTargetCalculator consumptionTargetCalculator =
                        new EqualConsumptionTargetCalculator(k8sTargetQueueSettingsProvider, capacityCalculatorBase);
                final StagingTargetPairProvider stagingTargetPairProvider = new StagingTargetPairProvider();

                final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(
                        queuesApi,
                        connectionFactory,
                        consumptionTargetCalculator,
                        stagingTargetPairProvider,
                        10000,
                        600000,
                        true,
                        false,
                        false,
                        Map.of(RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE, RabbitQueueConstants.RABBIT_PROP_QUEUE_TYPE_QUORUM));

                lowLevelDistributor.runOnce(connection);

                mockK8sServer.shutdown();
            }
        }
    }
}

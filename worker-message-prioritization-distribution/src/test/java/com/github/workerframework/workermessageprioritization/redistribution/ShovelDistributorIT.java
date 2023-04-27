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
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.AMQP;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class ShovelDistributorIT extends DistributorTestBase {

    @Test
    public void twoStagingQueuesTest() throws Exception {

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueue1Name = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);
        
        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();
            channel.basicQos(10, true);

            channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(targetQueueName, true, false, false, Collections.emptyMap());
            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final String body = gson.toJson(new Object());

            channel.basicConsume(targetQueueName, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    super.handleDelivery(consumerTag, envelope, properties, body);
                    try {
                        Thread.sleep(50);
                        channel.basicAck(envelope.getDeliveryTag(), false);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            
            for(int outerIndex = 0; outerIndex < 1000; outerIndex ++) {
                for(int index = 0; index < 10; index++) {
                    channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
//                    Thread.sleep(50);
                }
//                Thread.sleep(1000 * 10);                
            }

            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider());

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

            while(true) {

                shovelDistributor.runOnce();
                Thread.sleep(1000 * 5);

//            if(queueContainsNumMessages(stagingQueue1Name, 0).call()) {
//                break;
//            }

            }            
            
        }




        
    }
}

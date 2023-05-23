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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class SrcPrefetchCountInvestigationIT extends DistributorTestBase {
    //@Test
    public void srcPrefetchCountTest() throws TimeoutException, IOException, InterruptedException {

        try(final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();

            channel.queueDeclare("my-src-queue", true, false, false, Collections.emptyMap());
            channel.queueDeclare("my-dst-queue", true, false, false, Collections.emptyMap());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println("Received message: " + message);
                }
            };

            channel.basicConsume("my-dst-queue", true, consumer);

            final int numMessages = 10000;

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            for (int i = 0; i < numMessages; i++) {
                channel.basicPublish("", "my-src-queue", properties, gson.toJson(new Object()).getBytes(StandardCharsets.UTF_8));
            }

            await().alias(String.format("Waiting for my-src-queue to fill up with %s messages", numMessages))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(queueContainsNumMessages("my-src-queue", numMessages));

            for (int messageCount = numMessages; messageCount > 0; messageCount = messageCount - 1000) {

                await().alias(String.format("Waiting for my-src-queue to contain %s messages", messageCount))
                        .atMost(100, SECONDS)
                        .pollInterval(Duration.ofSeconds(1))
                        .until(queueContainsNumMessages("my-src-queue", messageCount));

                await().alias("Waiting for shovel named my-dst-queue to be deleted")
                        .atMost(100, SECONDS)
                        .pollInterval(Duration.ofSeconds(1))
                        .until(shovelIsDeleted("my-dst-queue"));

                final Shovel shovel = new Shovel();
                shovel.setAckMode("on-confirm");
                shovel.setSrcQueue("my-src-queue");
                shovel.setSrcUri("amqp://");
                shovel.setDestQueue("my-dst-queue");
                shovel.setDestUri("amqp://");
                shovel.setSrcDeleteAfter(1000);
                shovel.setSrcPrefetchCount(1000);

                shovelsApi.getApi().putShovel("/", "my-src-queue", new Component<>("shovel", "my-dst-queue", shovel));

                final List<Queue> queues = queuesApi.getApi().getQueues();

                for (final Queue queue : queues) {
                    System.out.println(queue.getName() + " " + queue.getMessage_stats());
                }
            }
        }
    }
}

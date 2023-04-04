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
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.redistribution.shovel.ShovelRunningTooLongChecker;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class ShovelRunningTooLongCheckerIT extends DistributorTestBase
{
    @Test
    public void shovelRunningTooLongShouldBeDeletedTest() throws TimeoutException, IOException, InterruptedException
    {
        // Create a staging queue and a target queue

        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueueName = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);

        try (final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();
            channel.queueDeclare(stagingQueueName, true, false, false, Collections.emptyMap());
            channel.queueDeclare(targetQueueName, true, false, false, Collections.emptyMap());
        }

        // Create a shovel that will stay in 'running' state forever (because we haven't supplied a src-delete-after value)

        final Shovel shovel = new Shovel();
        shovel.setAckMode("on-confirm");
        shovel.setSrcQueue(stagingQueueName);
        shovel.setSrcUri("amqp://");
        shovel.setDestQueue(targetQueueName);
        shovel.setDestUri("amqp://");

        shovelsApi.getApi().putShovel("/", stagingQueueName, new Component<>("shovel", stagingQueueName, shovel));

        // Verify the shovel is in a 'running' state

        RetrievedShovel retrievedShovel = null;

        for (int attempt = 0; attempt < 10; attempt++) {

            retrievedShovel = shovelsApi
                    .getApi()
                    .getShovels()
                    .stream()
                    .filter(s -> s.getName().equals(stagingQueueName))
                    .findFirst()
                    .get();

            if (retrievedShovel.getState() != ShovelState.RUNNING) {
                // Shovel not in a 'running' state yet, wait a bit and check again
                Thread.sleep(1000 * 10);
            }
        }

        Assert.assertEquals("Shovel should be in 'running' state", ShovelState.RUNNING, retrievedShovel.getState());

        // Run the ShovelRunningTooLongChecker to delete the shovel that has been running too long

        final long shovelRunningTooLongTimeoutMilliseconds = 1L;

        final ScheduledExecutorService shovelRunningTooLongCheckerExecutorService = Executors.newSingleThreadScheduledExecutor();

        shovelRunningTooLongCheckerExecutorService.scheduleAtFixedRate(
                new ShovelRunningTooLongChecker(
                        shovelsApi,
                        getNodeSpecificShovelsApiCache(),
                        "/",
                        shovelRunningTooLongTimeoutMilliseconds,
                        1L),
                0,
                1L,
                TimeUnit.MILLISECONDS);

        try {
            await().alias(String.format("Waiting for shovel named %s to be deleted", stagingQueueName))
                    .atMost(100, SECONDS)
                    .pollInterval(Duration.ofSeconds(1))
                    .until(shovelIsDeleted(stagingQueueName));
        } finally {
            shovelRunningTooLongCheckerExecutorService.shutdownNow();
        }
    }
}

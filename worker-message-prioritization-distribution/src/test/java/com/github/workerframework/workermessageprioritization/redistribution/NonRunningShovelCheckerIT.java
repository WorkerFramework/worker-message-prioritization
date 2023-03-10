/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.redistribution.shovel.NonRunningShovelChecker;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

public class NonRunningShovelCheckerIT extends DistributorTestBase
{
    @Test
    public void nonRunningShovelShouldBeDeletedTest() throws TimeoutException, IOException, InterruptedException
    {
        // Create a staging queue and a target queue
        final String targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final String stagingQueueName = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);

        try (final Connection connection = connectionFactory.newConnection()) {
            final Channel channel = connection.createChannel();
            channel.queueDeclare(stagingQueueName, true, false, false, Collections.emptyMap());
            channel.queueDeclare(targetQueueName, true, false, false, Collections.emptyMap());
        }

        // Create a bad shovel that never gets into a 'running' state
        final Shovel shovel = new Shovel();
        shovel.setAckMode("on-confirm");
        shovel.setSrcQueue(stagingQueueName);
        shovel.setSrcUri("amqp://BADURI");
        shovel.setDestQueue(targetQueueName);
        shovel.setDestUri("amqp://BADURI");

        shovelsApi.getApi().putShovel("/", stagingQueueName, new Component<>("shovel", stagingQueueName, shovel));

        // Verify the bad shovel is not in a 'running' state
        final RetrievedShovel retrievedShovel = shovelsApi
            .getApi()
            .getShovels()
            .stream()
            .filter(s -> s.getName().equals(stagingQueueName))
            .findFirst()
            .get();
        Assert.assertNotEquals("Bad shovel should not be in 'running' state", ShovelState.RUNNING, retrievedShovel.getState());

        // Run the ShovelStateChecker to delete the bad shovel.
        final NonRunningShovelChecker nonRunningShovelChecker = new NonRunningShovelChecker(shovelsApi, "/", 1L, 1L);
        nonRunningShovelChecker.run(); // First run() sets the timeObservedInNonRunningState to Instant.now()
        Thread.sleep(2000);       // Wait 2 seconds
        nonRunningShovelChecker.run(); // Second run() should see that the timeout of 1 millisecond has been reached, and delete the shovel

        // Verify the bad shovel has been deleted
        Optional<RetrievedShovel> retrievedShovelAfterDeletion;
        for (int attempt = 0; attempt < 10; attempt++) {
            retrievedShovelAfterDeletion = shovelsApi
                .getApi()
                .getShovels()
                .stream()
                .filter(s -> s.getName().equals(stagingQueueName))
                .findFirst();

            if (!retrievedShovelAfterDeletion.isPresent()) {
                // Test passed
                return; 
            } else {
                // Shovel not deleted yet, wait a bit and check again
                Thread.sleep(1000 * 10);
            }
        }

        Assert.fail("Shovel named " + stagingQueueName + " should have been deleted but wasn't");
    }
}

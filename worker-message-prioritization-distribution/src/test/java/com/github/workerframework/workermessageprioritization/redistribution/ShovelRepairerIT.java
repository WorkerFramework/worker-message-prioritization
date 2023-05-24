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
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelToCreate;
import com.github.workerframework.workermessageprioritization.redistribution.shovel.ShovelRepairer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class ShovelRepairerIT extends DistributorTestBase
{
    @Test
    public void deleteShovelTest() throws TimeoutException, IOException
    {
        // Create a shovel in 'RUNNING' state
        final RetrievedShovel retrievedShovel = createRunningShovel();

        // Delete the shovel
        final boolean shovelRepairerResult = ShovelRepairer.deleteShovel(retrievedShovel, shovelsApi.getApi(), null, VHOST);

        // Assert that ShovelRepairer reported success
        Assert.assertTrue("Expected result of ShovelRepairer.deleteShovel() to be true but was false", shovelRepairerResult);

        // Assert that the shovel is no longer present
        await().alias(String.format("Waiting for shovel named %s to be deleted", retrievedShovel.getName()))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(shovelIsDeleted(retrievedShovel.getName()));
    }

    @Test
    public void restartShovelTest() throws TimeoutException, IOException
    {
        // Create a shovel in 'RUNNING' state
        final RetrievedShovel retrievedShovel = createRunningShovel();

        // Make a note of the original shovel timestamp
        final long originalShovelTimestamp = retrievedShovel.getTimestamp().getTime();

        // Restart the shovel
        final boolean shovelRepairerResult = ShovelRepairer.restartShovel(retrievedShovel, shovelsApi.getApi(), null, VHOST);

        // Assert that the shovel was restarted successfully
        Assert.assertTrue("Expected result of ShovelRepairer.restartShovel() to be true but was false", shovelRepairerResult);

        // Get the shovel after being restarted
        final RetrievedShovel retrievedShovelAfterBeingRestarted = shovelsApi
                .getApi()
                .getShovels(VHOST)
                .stream()
                .filter(s -> s.getName().equals(retrievedShovel.getName()))
                .findFirst()
                .get();

        // Make a note of the restarted shovel timestamp
        final long restartedShovelTimestamp = retrievedShovelAfterBeingRestarted.getTimestamp().getTime();

        // Assert that the restarted shovel timestamp is greater than the original shovel timestamp
        Assert.assertTrue("Expected the restarted shovel timestamp to be greater than the original shovel timestamp",
                restartedShovelTimestamp > originalShovelTimestamp);
    }

    @Test
    public void recreateShovelTest() throws TimeoutException, IOException
    {
        // Create a shovel in 'RUNNING' state
        final RetrievedShovel retrievedShovel = createRunningShovel();

        // Recreate the shovel
        final boolean shovelRepairerResult = ShovelRepairer.recreateShovel(retrievedShovel, shovelsApi.getApi(), null, VHOST, "amqp://");

        // Assert that ShovelRepairer reported success
        Assert.assertTrue("Expected result of ShovelRepairer.recreateShovel() to be true but was false", shovelRepairerResult);

        // Assert that the shovel is no longer present (because we recreate a shovel with a src-delete-after of 0, it should be deleted
        // immediately - we do this because sometimes in production deleting or restarting a bad shovel does not work to get it unstuck,
        // but recreating it does)
        await().alias(String.format("Waiting for shovel named %s to be deleted", retrievedShovel.getName()))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(shovelIsDeleted(retrievedShovel.getName()));
    }

    private RetrievedShovel createRunningShovel() throws IOException, TimeoutException
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
        final ShovelToCreate shovelToCreate = new ShovelToCreate();
        shovelToCreate.setAckMode("on-confirm");
        shovelToCreate.setSrcQueue(stagingQueueName);
        shovelToCreate.setSrcUri("amqp://");
        shovelToCreate.setDestQueue(targetQueueName);
        shovelToCreate.setDestUri("amqp://");

        shovelsApi.getApi().putShovel("/", stagingQueueName, new Component<>("shovel", stagingQueueName, shovelToCreate));

        await().alias(String.format("Waiting for shovel named %s to be created", stagingQueueName))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(shovelIsCreated(stagingQueueName));

        await().alias(String.format("Waiting for shovel named %s to get into a 'RUNNING' state", stagingQueueName))
                .atMost(100, SECONDS)
                .pollInterval(Duration.ofSeconds(1))
                .until(shovelIsInState(stagingQueueName, ShovelState.RUNNING));

        // Return the shovel
        return shovelsApi
                .getApi()
                .getShovels(VHOST)
                .stream()
                .filter(s -> s.getName().equals(stagingQueueName))
                .findFirst()
                .get();
    }
}

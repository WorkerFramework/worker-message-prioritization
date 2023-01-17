/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.shovel.ShovelDistributor;
import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.AMQP;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class ShovelDistributorIT extends DistributorTestBase {

    @Test
    public void twoStagingQueuesTest() throws TimeoutException, IOException, InterruptedException {

        final var targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final var stagingQueue1Name = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);
        final var stagingQueue2Name = getStagingQueueName(targetQueueName, T2_STAGING_QUEUE_NAME);
        
        try(final var connection = connectionFactory.newConnection()) {
            final var channel = connection.createChannel();

            channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(stagingQueue2Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(targetQueueName, true, false, false, Collections.emptyMap());
            final var properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final var body = gson.toJson(new Object());

            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));

            //TODO Await publish confirms to ensure messages are published before running the test.
        }

        final var consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider());

        final var shovelDistributor = new ShovelDistributor(queuesApi, shovelsApi,
                consumptionTargetCalculator);

        Queue targetQueue = null;
        for(var attempt = 0; attempt < 10; attempt ++) {
            shovelDistributor.runOnce();

            targetQueue = queuesApi.getApi().getQueue("/", targetQueueName);
            if(targetQueue.getMessages() > 0) {
                break;
            }
            
            Thread.sleep(1000 * 10);
        }

        Assert.assertNotNull("Target queue was not found via REST API", targetQueue);
        final var stagingQueue1 = queuesApi.getApi().getQueue("/", stagingQueue1Name);
        final var stagingQueue2 = queuesApi.getApi().getQueue("/", stagingQueue2Name);
        Assert.assertEquals("Two staged messages should be on target queue", 2L, targetQueue.getMessages());
        Assert.assertEquals("1st Staging queue should be empty", 0L, stagingQueue1.getMessages());
        Assert.assertEquals("2n Staging queue should be empty", 0L, stagingQueue2.getMessages());
        
    }
}

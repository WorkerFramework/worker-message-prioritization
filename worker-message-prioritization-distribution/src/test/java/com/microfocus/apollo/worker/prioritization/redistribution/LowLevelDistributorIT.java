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
package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.lowlevel.LowLevelDistributor;
import com.microfocus.apollo.worker.prioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.microfocus.apollo.worker.prioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.AMQP;
import org.junit.Assert;
import org.junit.Test;
import retrofit.RetrofitError;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributorIT extends DistributorTestBase {
    
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
            
        }
        
        final var consumptionTargetCalculator = 
                new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider());
        final var stagingTargetPairProvider = new StagingTargetPairProvider();
        final var lowLevelDistributor = new LowLevelDistributor(queuesApi, connectionFactory, 
                consumptionTargetCalculator, stagingTargetPairProvider);

        Queue targetQueue = null;
        try(final var connection = connectionFactory.newConnection()) {
            for(var attempt = 0; attempt < 10; attempt ++) {
                lowLevelDistributor.runOnce(connection);

                targetQueue = queuesApi.getApi().getQueue("/", targetQueueName);
                if(targetQueue.getMessages() > 0) {
                    break;
                }

                Thread.sleep(1000 * 10);
            }
        }

        Assert.assertNotNull("Target queue was not found via REST API", targetQueue);
        final var stagingQueue1 = queuesApi.getApi().getQueue("/", stagingQueue1Name);
        final var stagingQueue2 = queuesApi.getApi().getQueue("/", stagingQueue2Name);
        Assert.assertEquals("Two staged messages should be on target queue", 2L, targetQueue.getMessages());
        Assert.assertEquals("1st Staging queue should be empty", 0L, stagingQueue1.getMessages());
        Assert.assertEquals("2n Staging queue should be empty", 0L, stagingQueue2.getMessages());
    }
}

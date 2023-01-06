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

import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.shovel.ShovelDistributor;
import com.microfocus.apollo.worker.prioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.AMQP;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class ShovelDistributorIT extends DistributorTestBase {

    @Test
    @Ignore
    public void twoStagingQueuesTest() throws TimeoutException, IOException {

        try(final var connection = connectionFactory.newConnection()) {
            final var channel = connection.createChannel();

            channel.queueDeclare(T1_STAGING_QUEUE_NAME, true, false, false, Collections.emptyMap());
            channel.queueDeclare(T2_STAGING_QUEUE_NAME, true, false, false, Collections.emptyMap());
            channel.queueDeclare(TARGET_QUEUE_NAME, true, false, false, Collections.emptyMap());

            final var properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final var body = gson.toJson(new Object());

            channel.basicPublish("", T1_STAGING_QUEUE_NAME, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", T2_STAGING_QUEUE_NAME, properties, body.getBytes(StandardCharsets.UTF_8));

            //TODO Await publish confirms to ensure messages are published before running the test.
        }

        final var consumptionTargetCalculator =
                new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider());

        final var shovelDistributor = new ShovelDistributor(queuesApi, shovelsApi,
                consumptionTargetCalculator);

        //TODO Stop the distributor?
        shovelDistributor.run();
    }
}

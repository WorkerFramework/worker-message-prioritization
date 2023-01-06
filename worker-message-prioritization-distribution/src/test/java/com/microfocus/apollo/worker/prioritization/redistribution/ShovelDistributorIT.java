package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.FixedTargetQueueCapacityProvider;
import com.microfocus.apollo.worker.prioritization.redistribution.shovel.ShovelDistributor;
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

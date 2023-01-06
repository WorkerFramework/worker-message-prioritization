package com.microfocus.apollo.worker.prioritization.redistribution;

import com.google.gson.Gson;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.FixedTargetQueueCapacityProvider;
import com.microfocus.apollo.worker.prioritization.redistribution.lowlevel.LowLevelDistributor;
import com.microfocus.apollo.worker.prioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributorIT extends DistributorTestBase {
    
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
        final var stagingTargetPairProvider = new StagingTargetPairProvider();
        final var lowLevelDistributor = new LowLevelDistributor(queuesApi, connectionFactory, 
                consumptionTargetCalculator, stagingTargetPairProvider);
        
        //TODO Stop the distributor?
        lowLevelDistributor.run();
    }
}

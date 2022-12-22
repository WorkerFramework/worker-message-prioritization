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

import com.microfocus.apollo.worker.prioritization.rabbitmq.Component;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RetrievedShovel;
import com.microfocus.apollo.worker.prioritization.rabbitmq.Shovel;
import com.microfocus.apollo.worker.prioritization.rabbitmq.ShovelsApi;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.FixedTargetQueueCapacityProvider;
import com.microfocus.apollo.worker.prioritization.redistribution.lowlevel.LowLevelDistributor;
import com.microfocus.apollo.worker.prioritization.redistribution.shovel.ShovelDistributor;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;

public class PocTests {

    @Test
    @Ignore
    public void runRoundRobin() throws IOException, TimeoutException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setHost("david-cent01.swinfra.net");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");
        
//        final Connection connection = connectionFactory.newConnection();

        final RabbitManagementApi<QueuesApi> queuesApi = 
                new RabbitManagementApi<>(QueuesApi.class, 
                        "http://david-cent01.swinfra.net:15672/", "guest", "guest");
        
        final LowLevelDistributor lowLevelDistributor = 
                new LowLevelDistributor(queuesApi, connectionFactory, new EqualConsumptionTargetCalculator(
                        new FixedTargetQueueCapacityProvider()
                ));
        
        lowLevelDistributor.run();
    }


    @Test
    @Ignore
    public void shovelTest() throws IOException, TimeoutException {

        final RabbitManagementApi<ShovelsApi> shovelsApi =
                new RabbitManagementApi<>(ShovelsApi.class,
                        "http://david-cent01.swinfra.net:15672/", "guest", "guest");

        final List<RetrievedShovel> s = shovelsApi.getApi().getShovels();

        shovelsApi.getApi().delete("/", "dataprocessing-classification-in»/michaelb01/enrichment-workflow");

        final Shovel shovel = new Shovel();
        shovel.setSrcDeleteAfter(1);
        shovel.setAckMode("on-confirm");
        shovel.setSrcQueue("dataprocessing-classification-in»/michaelb01/enrichment-workflow");
        shovel.setSrcUri("amqp://");
        shovel.setDestQueue("dataprocessing-classification-in");
        shovel.setDestUri("amqp://");
        
        
        final RetrievedShovel newShovel = shovelsApi.getApi().putShovel("/", "s1", new Component<>("shovel", "s1", shovel));
    }

    @Test
    @Ignore
    public void shovelDistributorTest() throws IOException, TimeoutException, InterruptedException {

        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://david-cent01.swinfra.net:15672/", "guest", "guest");

        final RabbitManagementApi<ShovelsApi> shovelsApi =
                new RabbitManagementApi<>(ShovelsApi.class,
                        "http://david-cent01.swinfra.net:15672/", "guest", "guest");

        final ShovelDistributor shovelDistributor = new ShovelDistributor(
                queuesApi, shovelsApi, 1000, new EqualConsumptionTargetCalculator(
                new FixedTargetQueueCapacityProvider()
        ));

        //                        final RetrievedShovel retrievedShovel = shovelsApi.getApi().getShovel("/", sourceQueue.getName());
//                        shovelsApi.getApi().restartShovel("/", sourceQueue.getName());
//        shovelsApi.getApi().delete("/", sourceQueue.getName());


        while(true) {
            shovelDistributor.run();
            
            Thread.sleep(1000 *10);
            
        }
    }
    
}
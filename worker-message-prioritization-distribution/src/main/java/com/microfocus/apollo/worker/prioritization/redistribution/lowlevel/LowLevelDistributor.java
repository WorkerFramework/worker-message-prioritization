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
package com.microfocus.apollo.worker.prioritization.redistribution.lowlevel;

import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.microfocus.apollo.worker.prioritization.redistribution.MessageDistributor;
import com.microfocus.apollo.worker.prioritization.redistribution.consumption.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributor extends MessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelDistributor.class);

    private ShutdownSignalException shutdownSignalException = null;
    private final ConsumptionTargetCalculator consumptionTargetCalculator;
    private final ConnectionFactory connectionFactory;

    public LowLevelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                               final ConnectionFactory connectionFactory,
                               final ConsumptionTargetCalculator consumptionTargetCalculator) {
        super(queuesApi);
        this.connectionFactory = connectionFactory;
        this.consumptionTargetCalculator = consumptionTargetCalculator;
    }
    
    public static void main(String[] args) throws IOException, TimeoutException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(args[0]);
        connectionFactory.setUsername(args[1]);
        connectionFactory.setPassword(args[2]);
        connectionFactory.setPort(Integer.parseInt(args[3]));
        connectionFactory.setVirtualHost("/");
        
        //https://www.rabbitmq.com/api-guide.html#java-nio
        //connectionFactory.useNio();
                
        final int managementPort = Integer.parseInt(args[4]);
        final long targetQueueMessageLimit = Long.parseLong(args[5]);
        
        //TODO ManagementApi does not necessarily have same host, username and password, nor use http
        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/", 
                        connectionFactory.getUsername(), connectionFactory.getPassword());

        final LowLevelDistributor lowLevelDistributor =
                new LowLevelDistributor(queuesApi, connectionFactory, 
                        new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider()));
        
        lowLevelDistributor.run();
    }
    
    public void run() throws IOException, TimeoutException {
        
        final Connection connection = connectionFactory.newConnection();
        
        //This loop retrieves the current list of queues from RabbitMQ
        //creating MessageTargets, when needed, and registering new MessageSources when encountered
        while(true) {

            final Set<DistributorWorkItem> distributorWorkItems = getDistributorWorkItems();

            for(final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
                final MessageMover messageMover = new MessageMover(connection, distributorWorkItem, 
                        consumptionTargetCalculator);
                
                messageMover.start();

            }
            
            if(shutdownSignalException != null) {
                break;
            }
            
            try {
                Thread.sleep(1000 * 10);
            } catch (final InterruptedException e) {
                LOGGER.warn("Exiting {}", e.getMessage());
                return;
            }
        }
        
    }
}

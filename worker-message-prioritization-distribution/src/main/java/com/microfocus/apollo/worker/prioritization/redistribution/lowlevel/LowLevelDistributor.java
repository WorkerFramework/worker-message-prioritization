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
import com.microfocus.apollo.worker.prioritization.redistribution.DistributorWorkItem;
import com.microfocus.apollo.worker.prioritization.redistribution.MessageDistributor;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class LowLevelDistributor extends MessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LowLevelDistributor.class);

    private ShutdownSignalException shutdownSignalException = null;
    private final long targetQueueMessageLimit;
    private final ConnectionFactory connectionFactory;
    private final ConcurrentHashMap<String, MessageTarget> messageTargets = new ConcurrentHashMap<>();

    public LowLevelDistributor(final RabbitManagementApi<QueuesApi> queuesApi,
                               final ConnectionFactory connectionFactory, final long targetQueueMessageLimit) {
        super(queuesApi);
        this.connectionFactory = connectionFactory;
        this.targetQueueMessageLimit = targetQueueMessageLimit;

    }
    
    public static void main(String[] args) throws IOException, TimeoutException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(args[0]);
        connectionFactory.setUsername(args[1]);
        connectionFactory.setPassword(args[2]);
        connectionFactory.setPort(Integer.parseInt(args[3]));
        connectionFactory.setVirtualHost("/");
        
        final int managementPort = Integer.parseInt(args[4]);
        final long targetQueueMessageLimit = Long.parseLong(args[5]);

//        final Connection connection = connectionFactory.newConnection();

        //TODO ManagementApi does not necessarily have same host, username and password, nor use http
        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/", 
                        connectionFactory.getUsername(), connectionFactory.getPassword());

        final LowLevelDistributor lowLevelDistributor =
                new LowLevelDistributor(queuesApi, connectionFactory, targetQueueMessageLimit);
        
        lowLevelDistributor.run();
    }
    
    public void run() throws IOException, TimeoutException {
        
        //This loop retrieves the current list of queues from RabbitMQ
        //creating MessageTargets, when needed, and registering new MessageSources when encountered
        while(true) {

            final Set<DistributorWorkItem> distributorWorkItems = getDistributorTargets();
            
            for(final DistributorWorkItem distributorWorkItem : distributorWorkItems) {
                final MessageTarget messageTarget;
                if(!messageTargets.containsKey(distributorWorkItem.getTargetQueue().getName())) {
                    messageTarget = new MessageTarget(targetQueueMessageLimit, connectionFactory, distributorWorkItem.getTargetQueue());
                    messageTargets.put(distributorWorkItem.getTargetQueue().getName(), messageTarget);
                }
                else {
                    messageTarget = messageTargets.get(distributorWorkItem.getTargetQueue().getName());
                    if (messageTarget.getShutdownSignalException() != null) {
                        shutdownSignalException = messageTarget.getShutdownSignalException();
                        messageTargets.remove(distributorWorkItem.getTargetQueue().getName());
                        continue;
                    }
                }
                messageTarget.run(distributorWorkItem.getTargetQueue(), distributorWorkItem.getStagingQueues());

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

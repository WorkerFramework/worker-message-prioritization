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

import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.microfocus.apollo.worker.prioritization.management.QueuesApi;
import com.microfocus.apollo.worker.prioritization.management.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.rerouting.MessageRouter;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RoundRobinMessageDistributor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinMessageDistributor.class);

    private ShutdownSignalException shutdownSignalException = null;
    private final long targetQueueMessageLimit;
    private final RabbitManagementApi<QueuesApi> queuesApi;
    private final Connection connection;
    private final ConcurrentHashMap<String, MessageTarget> messageTargets = new ConcurrentHashMap<>();

    public RoundRobinMessageDistributor(final RabbitManagementApi<QueuesApi> queuesApi, 
                                        final Connection connection, final long targetQueueMessageLimit) {
        this.queuesApi = queuesApi;
        this.connection = connection;
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

        final Connection connection = connectionFactory.newConnection();

        //TODO ManagementApi does not necessarily have same host, username and password, nor use http
        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/", 
                        connectionFactory.getUsername(), connectionFactory.getPassword());

        final RoundRobinMessageDistributor roundRobinMessageDistributor =
                new RoundRobinMessageDistributor(queuesApi, connection, targetQueueMessageLimit);
        
        roundRobinMessageDistributor.run();
    }
    
    public void run() throws IOException {
        
        final ExecutorService executorService = Executors.newWorkStealingPool();
        
        //This loop retrieves the current list of queues from RabbitMQ
        //creating MessageTargets, when needed, and registering new MessageSources when encountered
        while(true) {

            final List<Queue> queues = queuesApi.getApi().getQueues();
            
            final Set<Queue> messageTargetQueues = getMesssageTargetsQueues(queues);
            
            for(final Queue messageTargetQueue: messageTargetQueues) {
                final MessageTarget messageTarget;
                if(!messageTargets.containsKey(messageTargetQueue.getName())) {
                    messageTarget = new MessageTarget(targetQueueMessageLimit, connection, messageTargetQueue);
                    messageTarget.updateMessageSources(getMessageSourceQueues(messageTarget, queues));
                    messageTargets.put(messageTargetQueue.getName(), messageTarget);
                    messageTarget.initialise();
                    executorService.submit(messageTarget::start);
                }
                else {
                    messageTarget = messageTargets.get(messageTargetQueue.getName());
                    if (messageTarget.getShutdownSignalException() != null) {
                        shutdownSignalException = messageTarget.getShutdownSignalException();
                        messageTargets.remove(messageTargetQueue.getName());
                        continue;
                    }
                    messageTarget.updateQueueMetadata(messageTargetQueue);
                    messageTarget.updateMessageSources(getMessageSourceQueues(messageTarget, queues));
                }
            }
            
            if(shutdownSignalException != null) {
                try {
                    final boolean timedOut = executorService.awaitTermination(1, TimeUnit.MINUTES);
                    if(timedOut) {
                        LOGGER.warn("Timed out while awaiting completion.");
                    }
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
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

    private Set<Queue> getMesssageTargetsQueues(final List<Queue> queues) {

        return queues.stream()
                .filter(q ->
                        !q.getName().contains(MessageRouter.LOAD_BALANCED_INDICATOR)
                )
                .collect(Collectors.toSet());
        
    }
    
    private Set<Queue> getMessageSourceQueues(final MessageTarget messageTarget, final List<Queue> queues) {

        return queues.stream()
                .filter(q -> 
                        q.getName().startsWith(messageTarget.getTargetQueueName() + MessageRouter.LOAD_BALANCED_INDICATOR)
                )
                .collect(Collectors.toSet());
    }

    // tq dataprocessing-worker-entity-extract-in
    // sq dataprocessing-worker-entity-extract-in-t1-ingestion
    // sq dataprocessing-worker-entity-extract-in-t1-enrichment
    

}

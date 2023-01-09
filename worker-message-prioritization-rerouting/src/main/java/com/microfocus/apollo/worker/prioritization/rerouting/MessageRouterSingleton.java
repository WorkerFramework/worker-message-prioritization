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
package com.microfocus.apollo.worker.prioritization.rerouting;

import com.hpe.caf.worker.document.model.Document;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.microfocus.apollo.worker.prioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.microfocus.apollo.worker.prioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageRouterSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouterSingleton.class);
    private static Connection connection;    
    private static MessageRouter messageRouter;
    private static volatile boolean initAttempted = false;
    
    
    public static void init() {
        
        if(messageRouter != null || initAttempted) {
            return;
        }

        initAttempted = true;

        if(!Boolean.parseBoolean(System.getenv("CAF_WMP_ENABLED"))) {
            LOGGER.error("Ignored WMP init request, CAF_WMP_ENABLED evaluated to false.");
            return;
        }
        
        try {

            final ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUsername(System.getenv("CAF_RABBITMQ_USERNAME"));
            connectionFactory.setPassword(System.getenv("CAF_RABBITMQ_PASSWORD"));
            connectionFactory.setHost(System.getenv("CAF_RABBITMQ_HOST"));
            connectionFactory.setPort(Integer.parseInt(System.getenv("CAF_RABBITMQ_PORT")));
            connectionFactory.setVirtualHost("/");

            final String mgmtEndpoint = System.getenv("CAF_RABBITMQ_MGMT_URL");
            final String mgmtUsername = System.getenv("CAF_RABBITMQ_MGMT_USERNAME");
            final String mgmtPassword = System.getenv("CAF_RABBITMQ_MGMT_PASSWORD");
            final TargetQueueCapacityProvider targetQueueCapacityProvider = new FixedTargetQueueCapacityProvider();

            connection = connectionFactory.newConnection();

            final RabbitManagementApi<QueuesApi> queuesApi =
                    new RabbitManagementApi<>(QueuesApi.class, mgmtEndpoint, mgmtUsername, mgmtPassword);
            
            final var stagingQueueCreator = new StagingQueueCreator(connection.createChannel());

            messageRouter = new MessageRouter(queuesApi, "/", stagingQueueCreator, targetQueueCapacityProvider);
        }
        catch (final Throwable e) {
            LOGGER.error("Failed to initialise WMP - {}", e.toString());
        }
        
    }
    
    public static void route(final Document document) {
        if(messageRouter != null) {
            messageRouter.route(document);
        }
    }
    
    public static void close() throws IOException {
        if(connection != null) {
            connection.close();
        }
    }

}

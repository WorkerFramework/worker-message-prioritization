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
package com.microfocus.fas.worker.prioritization.rerouting;

import com.hpe.caf.worker.document.model.Document;
import com.microfocus.fas.worker.prioritization.management.QueuesApi;
import com.microfocus.fas.worker.prioritization.management.RabbitManagementApi;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MessageRouterSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouterSingleton.class);

    private static final Connection connection;    
    private static final MessageRouter messageRouter;
    
    static {
        
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(System.getenv("CAF_RABBITMQ_USERNAME"));
        connectionFactory.setPassword(System.getenv("CAF_RABBITMQ_PASSWORD"));
        connectionFactory.setHost(System.getenv("CAF_RABBITMQ_HOST"));
        connectionFactory.setPort(Integer.parseInt(System.getenv("CAF_RABBITMQ_PORT")));
        connectionFactory.setVirtualHost("/");

        final String mgmtEndpoint = System.getenv("CAF_RABBITMQ_MGMT_URL");
        final String mgmtUsername = System.getenv("CAF_RABBITMQ_MGMT_USERNAME");
        final String mgmtPassword = System.getenv("CAF_RABBITMQ_MGMT_PASSWORD");
        
        try {
            connection = connectionFactory.newConnection();

            final RabbitManagementApi<QueuesApi> queuesApi =
                    new RabbitManagementApi<>(QueuesApi.class, mgmtEndpoint, mgmtUsername, mgmtPassword);

            messageRouter = new MessageRouter(queuesApi, "/", connection.createChannel(), 1000);
        } catch (final IOException | TimeoutException e) {
            LOGGER.error("Failed to initialise - {}", e.toString());
            throw new RuntimeException(e);
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

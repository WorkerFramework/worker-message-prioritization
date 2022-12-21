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

import com.hpe.caf.worker.document.model.Application;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.Response;
import com.hpe.caf.worker.document.model.ResponseQueue;
import com.hpe.caf.worker.document.model.Task;
import com.microfocus.apollo.worker.prioritization.rabbitmq.Component;
import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PocTests {
    
    @Test
    @Ignore
    public void processDocumentMessageRouter() throws IOException, TimeoutException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        connectionFactory.setHost("david-cent01.swinfra.net");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("/");

        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://david-cent01.swinfra.net:15672/", "guest", "guest");
        
        final MessageRouter messageRouter = new MessageRouter(queuesApi,  "/", channel, -1);

        final Document document = mock(Document.class);
        when(document.getCustomData("tenantId")).thenReturn("poc-tenant");
        when(document.getCustomData("workflowName")).thenReturn("enrichment");
        final Task task = mock(Task.class);
        when(document.getTask()).thenReturn(task);
        final Response response = mock(Response.class);
        when(task.getResponse()).thenReturn(response);
        final ResponseQueue responseQueue = new MockResponseQueue();
//        responseQueue.set("dataprocessing-entity-extract-in");
        responseQueue.set("wmp-in");
        when(response.getSuccessQueue()).thenReturn(responseQueue);
        
        messageRouter.route(document);

    }
    
    private class MockResponseQueue implements ResponseQueue {
        
        private String name;

        @Override
        public void disable() {
            
        }
        
        @Override
        public String getName() {
            return name;
        }
        
        @Override
        public Response getResponse() {
            return null;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public void reset() {

        }

        @Override
        public void set(String s) {
            name = s;
        }
        
        @Override
        public Application getApplication() {
            return null;
        }
    }
    
}
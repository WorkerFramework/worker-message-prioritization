/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.workerframework.workermessageprioritization.rerouting;

import com.hpe.caf.worker.document.model.Application;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.Response;
import com.hpe.caf.worker.document.model.ResponseQueue;
import com.hpe.caf.worker.document.model.Task;
import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageRouterTests {
    
    @Test
    public void processDocumentMessageRouter() {
        
        @SuppressWarnings("unchecked")
        final RabbitManagementApi<QueuesApi> queuesApiWrapper = (RabbitManagementApi<QueuesApi>)mock(RabbitManagementApi.class);
        final QueuesApi queuesApi = mock(QueuesApi.class);
        final Queue mockQueue = mock(Queue.class);
        when(queuesApi.getQueue(anyString(), anyString())).thenReturn(mockQueue);
        when(queuesApiWrapper.getApi()).thenReturn(queuesApi);
        
        final TargetQueueCapacityProvider targetQueueCapacityProvider = mock(TargetQueueCapacityProvider.class) ;
        final StagingQueueCreator stagingQueueCreator = mock(StagingQueueCreator.class);
        final MessageRouter messageRouter = new MessageRouter(queuesApiWrapper,  "/", stagingQueueCreator, 
                targetQueueCapacityProvider);

        final Document document = mock(Document.class);
        when(document.getCustomData("tenantId")).thenReturn("poc-tenant");
        when(document.getCustomData("workflowName")).thenReturn("enrichment");
        final Task task = mock(Task.class);
        when(document.getTask()).thenReturn(task);
        final Response response = mock(Response.class);
        when(task.getResponse()).thenReturn(response);
        final ResponseQueue responseQueue = new MockResponseQueue();
        responseQueue.set("dataprocessing-entity-extract-in");
        when(response.getSuccessQueue()).thenReturn(responseQueue);
        
        messageRouter.route(document);

        final String expectedSuccessQueue = 
                "dataprocessing-entity-extract-in" + MessageRouter.LOAD_BALANCED_INDICATOR + "/poc-tenant/enrichment";
        Assert.assertEquals("New success queue is incorrect.", expectedSuccessQueue, responseQueue.getName());

    }
    
    private static class MockResponseQueue implements ResponseQueue {
        
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
/*
 * Copyright 2022-2023 Micro Focus or one of its affiliates.
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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.google.gson.Gson;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.rabbitmq.client.ConnectionFactory;

public class DistributorTestBase {

    protected static final String T1_STAGING_QUEUE_NAME = "tenant1";
    protected static final String T2_STAGING_QUEUE_NAME = "tenant2";
    protected static final String TARGET_QUEUE_NAME = "target";
    
    protected final Gson gson = new Gson();
    protected ConnectionFactory connectionFactory;
    protected int managementPort;
    protected RabbitManagementApi<QueuesApi> queuesApi;
    protected RabbitManagementApi<ShovelsApi> shovelsApi;

    public DistributorTestBase() {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");
        this.connectionFactory = connectionFactory;

        managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                        connectionFactory.getUsername(), connectionFactory.getPassword());
        
        shovelsApi = new RabbitManagementApi<>(ShovelsApi.class, 
                "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                connectionFactory.getUsername(), connectionFactory.getPassword());

    }
    
    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }
    
    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + MessageDistributor.LOAD_BALANCED_INDICATOR + stagingQueueName;
    }
    
    
}

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
package com.github.workerframework.workermessageprioritization.rerouting;

import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.rabbitmq.client.ConnectionFactory;

public class RerouterTestBase {

    protected static final String TARGET_QUEUE_NAME = "target";
    protected static final String T1_STAGING_QUEUE_NAME = "tenant1";

    protected ConnectionFactory connectionFactory;
    protected RabbitManagementApi<QueuesApi> queuesApi;
    protected RabbitManagementApi<QueuesApi> cachingQueuesApi;

    public RerouterTestBase() {

        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");

        final int managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        queuesApi
                = new RabbitManagementApi<>(QueuesApi.class,
                "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                connectionFactory.getUsername(), connectionFactory.getPassword());

        cachingQueuesApi
                = new RabbitManagementApi<>(QueuesApi.class,
                "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                connectionFactory.getUsername(), connectionFactory.getPassword(), 10000);
    }

    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }

    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + MessageRouter.LOAD_BALANCED_INDICATOR + stagingQueueName;
    }
}

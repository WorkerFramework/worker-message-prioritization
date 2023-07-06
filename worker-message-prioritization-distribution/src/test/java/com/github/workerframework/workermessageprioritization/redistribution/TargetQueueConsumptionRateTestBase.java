/*
 * Copyright 2022-2023 Open Text.
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

import com.github.workerframework.workermessageprioritization.rabbitmq.NodesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TargetQueueConsumptionRateTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetQueueConsumptionRateTestBase.class);

    protected ConnectionFactory connectionFactory;
    protected int managementPort;
    protected RabbitManagementApi<QueuesApi> queuesApi;
    protected RabbitManagementApi<ShovelsApi> shovelsApi;
    protected RabbitManagementApi<NodesApi> nodesApi;
    private final String managementUrl;

    public TargetQueueConsumptionRateTestBase() {
        connectionFactory = new ConnectionFactory();

        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");

        this.connectionFactory = connectionFactory;

        managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        managementUrl = "http://" + connectionFactory.getHost() + ":" + managementPort + "/";

        queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        managementUrl,
                        connectionFactory.getUsername(), connectionFactory.getPassword());
        
        shovelsApi = new RabbitManagementApi<>(ShovelsApi.class,
                managementUrl,
                connectionFactory.getUsername(), connectionFactory.getPassword());

        nodesApi = new RabbitManagementApi<>(NodesApi.class,
                managementUrl,
                connectionFactory.getUsername(), connectionFactory.getPassword());
    }
}

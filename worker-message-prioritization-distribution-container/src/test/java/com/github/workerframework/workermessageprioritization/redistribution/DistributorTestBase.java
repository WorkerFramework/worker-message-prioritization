/*
 * Copyright 2022-2025 Open Text.
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

import java.util.concurrent.Callable;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApiImpl;
import com.rabbitmq.client.ConnectionFactory;

public class DistributorTestBase {

    protected static final String T1_STAGING_QUEUE_NAME = "tenant1";
    protected static final String T2_STAGING_QUEUE_NAME = "tenant2";
    protected static final String TARGET_QUEUE_NAME = "target";

    protected ConnectionFactory connectionFactory;
    protected int managementPort;
    protected QueuesApiImpl queuesApi;

    public DistributorTestBase() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");

        managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        queuesApi =
                new QueuesApiImpl(
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                        connectionFactory.getUsername(), connectionFactory.getPassword());

    }

    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }

    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + MessageDistributor.LOAD_BALANCED_INDICATOR + stagingQueueName;
    }

    protected Callable<Boolean> queueExists(final String queueName) {
        return () -> queuesApi.getQueues().stream().map(Queue::getName).anyMatch(name -> name.equals(queueName));
    }

    protected Callable<Boolean> queueContainsNumMessages(final String queueName, final int numMessages) {
        return () -> queuesApi.getQueue("/", queueName).getMessages() == numMessages;
    }
}

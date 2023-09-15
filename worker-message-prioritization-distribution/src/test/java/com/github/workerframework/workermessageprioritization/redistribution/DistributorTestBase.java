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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.github.workerframework.workermessageprioritization.rabbitmq.NodesApi;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.rabbitmq.client.ConnectionFactory;

public class DistributorTestBase {

    protected static final String T1_STAGING_QUEUE_NAME = "tenant1";
    protected static final String T2_STAGING_QUEUE_NAME = "tenant2";
    protected static final String TARGET_QUEUE_NAME = "target";
    protected static final String VHOST = "/";
    
    protected final Gson gson = new Gson();
    protected ConnectionFactory connectionFactory;
    protected int managementPort;
    protected RabbitManagementApi<QueuesApi> queuesApi;
    protected RabbitManagementApi<NodesApi> nodesApi;
    private final String managementUrl;
    private static final String CAF_RABBITMQ_HOST = "CAF_RABBITMQ_HOST";
    private static final String CAF_RABBITMQ_PORT = "CAF_RABBITMQ_PORT";
    private static final String CAF_RABBITMQ_USERNAME = "CAF_RABBITMQ_USERNAME";
    private static final String CAF_RABBITMQ_PASSWORD = "CAF_RABBITMQ_PASSWORD";
    private static final String CAF_RABBITMQ_CTRL_PORT = "CAF_RABBITMQ_CTRL_PORT";

    public DistributorTestBase() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(getEnvOrDefault(CAF_RABBITMQ_HOST, "localhost"));
        connectionFactory.setPort(Integer.parseInt(getEnvOrDefault(CAF_RABBITMQ_PORT, "25672")));
        connectionFactory.setUsername(getEnvOrDefault(CAF_RABBITMQ_USERNAME, "guest"));
        connectionFactory.setPassword(getEnvOrDefault(CAF_RABBITMQ_PASSWORD, "guest"));
        connectionFactory.setVirtualHost("/");
        this.connectionFactory = connectionFactory;

        managementPort = Integer.parseInt(getEnvOrDefault(CAF_RABBITMQ_CTRL_PORT, "25673"));

        managementUrl = "http://" + connectionFactory.getHost() + ":" + managementPort + "/";

        queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        managementUrl,
                        connectionFactory.getUsername(), connectionFactory.getPassword());

        nodesApi = new RabbitManagementApi<>(NodesApi.class,
                managementUrl,
                connectionFactory.getUsername(), connectionFactory.getPassword());
    }

    private static String getEnvOrDefault(final String name, final String defaultValue) {
        final String value = System.getenv(name);

        return !Strings.isNullOrEmpty(value) ? value : defaultValue;
    }
    
    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }
    
    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + MessageDistributor.LOAD_BALANCED_INDICATOR + stagingQueueName;
    }

    protected Callable<Boolean> queueContainsNumMessages(final String queueName, final int numMessages)
    {
        return () -> queuesApi.getApi().getQueue("/", queueName).getMessages() == numMessages;
    }
}

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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
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
    protected RabbitManagementApi<ShovelsApi> shovelsApi;
    private final String managementUrl;

    public DistributorTestBase() {
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

    }
    
    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }
    
    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + MessageDistributor.LOAD_BALANCED_INDICATOR + stagingQueueName;
    }

    protected LoadingCache<String,RabbitManagementApi<ShovelsApi>> getNodeSpecificShovelsApiCache() {
        return CacheBuilder
                .newBuilder()
                .expireAfterAccess(7, TimeUnit.DAYS)
                .build(new CacheLoader<String,RabbitManagementApi<ShovelsApi>>()
                {
                    @Override
                    public RabbitManagementApi<ShovelsApi> load(@Nonnull final String node)
                    {
                        // When running these integration tests, the 'node' value returned by the RabbitMQ Shovels API contains
                        // an ID rather than a host, for example:
                        //
                        // node:            rabbit@5d73b9c7a198
                        //
                        // whereas the management URL contains localhost:
                        //
                        // managementUrl:   http://localhost:49165/
                        //
                        // As such, we are unable to use the node value to build a node-specific management URL, as
                        // http://5d73b9c7a198:49165/ is not a valid host.
                        //
                        // So for the purposes of these tests, we will just use the standard Shovels API, rather than creating
                        // node-specific Shovels APIs.

                        return shovelsApi;
                    }
                });
    }

    protected Callable<Boolean> queueContainsNumMessages(final String queueName, final int numMessages)
    {
        return () -> queuesApi.getApi().getQueue("/", queueName).getMessages() == numMessages;
    }

    protected Callable<Boolean> shovelIsDeleted(final String shovelName)
    {
        return () -> !shovelsApi
                .getApi()
                .getShovels(VHOST)
                .stream()
                .filter(s -> s.getName().equals(shovelName))
                .findFirst()
                .isPresent();
    }
}

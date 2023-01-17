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
package com.github.workerframework.workermessageprioritization.redistribution.lowlevel;

import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.FixedTargetQueueCapacityProvider;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class LowLevelApplication {
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(args[0]);
        connectionFactory.setUsername(args[1]);
        connectionFactory.setPassword(args[2]);
        connectionFactory.setPort(Integer.parseInt(args[3]));
        connectionFactory.setVirtualHost("/");

        //https://www.rabbitmq.com/api-guide.html#java-nio
        //connectionFactory.useNio();

        final int managementPort = Integer.parseInt(args[4]);

        //TODO ManagementApi does not necessarily have same host, username and password, nor use http
        final RabbitManagementApi<QueuesApi> queuesApi =
                new RabbitManagementApi<>(QueuesApi.class,
                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                        connectionFactory.getUsername(), connectionFactory.getPassword());

        final LowLevelDistributor lowLevelDistributor =
                new LowLevelDistributor(queuesApi, connectionFactory,
                        new EqualConsumptionTargetCalculator(new FixedTargetQueueCapacityProvider()),
                        new StagingTargetPairProvider());

        lowLevelDistributor.run();
    }
}

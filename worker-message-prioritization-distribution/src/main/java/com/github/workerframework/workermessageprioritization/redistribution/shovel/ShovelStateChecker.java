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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class ShovelStateChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelStateChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final String rabbitMQVHost;
    private final long nonRunningShovelTimeoutMilliseconds;

    public ShovelStateChecker(
        final RabbitManagementApi<ShovelsApi> shovelsApi,
        final String rabbitMQVHost,
        final long nonRunningShovelTimeoutMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.rabbitMQVHost = rabbitMQVHost;
        this.nonRunningShovelTimeoutMilliseconds = nonRunningShovelTimeoutMilliseconds;
    }

    @Override
    public void run()
    {        
        final List<RetrievedShovel> retrievedShovels = shovelsApi.getApi().getShovels();

        LOGGER.debug("Read the following list of shovels from the RabbitMQ API: {}", retrievedShovels);

        for (final RetrievedShovel retrievedShovel : retrievedShovels) {

            if (retrievedShovel.getState() != ShovelState.RUNNING) {

                final Date shovelCreationTime = retrievedShovel.getTimestamp();
                final long shovelCreationTimeMilliseconds = shovelCreationTime.getTime();

                final Date timeNow = new Date();
                final long timeNowMilliseconds = timeNow.getTime();

                if (timeNowMilliseconds - shovelCreationTimeMilliseconds >= nonRunningShovelTimeoutMilliseconds) {   
                    LOGGER.error("Shovel named {} was created at {}. The time now is {}. "
                        + "The shovel is not yet in a 'running' state. It's current state is '{}'. "
                        + "As the non-running shovel timeout of {} milliseconds has been reached, the shovel is now going to be deleted. "
                        + "Please check the RabbitMQ logs for more details. "
                        + "Shovel creation will be attempted later if the shovel is still required.",
                                 retrievedShovel.getName(),
                                 shovelCreationTime,
                                 timeNow,
                                 retrievedShovel.getState().toString().toLowerCase(),
                                 nonRunningShovelTimeoutMilliseconds);
 
                    shovelsApi.getApi().delete(rabbitMQVHost, retrievedShovel.getName());
                } else {
                    LOGGER.debug("Shovel named {} was created at {}. The time now is {}. "
                        + "The shovel is not yet in a 'running' state. It's current state is '{}'. "
                        + "However, as the non-running shovel timeout of {} milliseconds has not been reached yet, "
                        + "the shovel will not be deleted at this time.",
                                 retrievedShovel.getName(),
                                 shovelCreationTime,
                                 timeNow,
                                 retrievedShovel.getState().toString().toLowerCase(),
                                 nonRunningShovelTimeoutMilliseconds);
                }
            }
        }
    }
}

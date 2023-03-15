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
package com.github.workerframework.workermessageprioritization.redistribution.shovel;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import net.jodah.expiringmap.ExpiringMap;

public final class ShovelStateChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelStateChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final Map<String, Instant> shovelNameToTimeObservedInNonRunningState;
    private final String rabbitMQVHost;
    private final long nonRunningShovelTimeoutMilliseconds;
    private final long nonRunningShovelTimeoutCheckIntervalMilliseconds;

    public ShovelStateChecker(
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final String rabbitMQVHost,
            final long nonRunningShovelTimeoutMilliseconds,
            final long nonRunningShovelTimeoutCheckIntervalMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.shovelNameToTimeObservedInNonRunningState = ExpiringMap.builder().expiration(12, TimeUnit.HOURS).build();
        this.rabbitMQVHost = rabbitMQVHost;
        this.nonRunningShovelTimeoutMilliseconds = nonRunningShovelTimeoutMilliseconds;
        this.nonRunningShovelTimeoutCheckIntervalMilliseconds = nonRunningShovelTimeoutCheckIntervalMilliseconds;
    }

    @Override
    public void run()
    {
        final List<RetrievedShovel> retrievedShovels;
        try {
            retrievedShovels = shovelsApi.getApi().getShovels();
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels, so unable to check if any shovels are in a " +
                            "non-running state and need to be deleted. Will try again during the next run in %d milliseconds",
                    nonRunningShovelTimeoutCheckIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }

        LOGGER.debug("Read the following list of shovels from the RabbitMQ API: {}", retrievedShovels);

        for (final RetrievedShovel retrievedShovel : retrievedShovels) {

            final String shovelName = retrievedShovel.getName();

            if (retrievedShovel.getState() != ShovelState.RUNNING) {

                final Instant timeObservedInNonRunningState = shovelNameToTimeObservedInNonRunningState
                    .computeIfAbsent(shovelName, s -> Instant.now());
                final long timeObservedInNonRunningStateMilliseconds = timeObservedInNonRunningState.toEpochMilli();

                final Instant timeNow = Instant.now();
                final long timeNowMilliseconds = timeNow.toEpochMilli();

                if (timeNowMilliseconds - timeObservedInNonRunningStateMilliseconds >= nonRunningShovelTimeoutMilliseconds) {   
                    LOGGER.error("Shovel named {} was observed in a non-running state at {}. The time now is {}. "
                        + "It's current state is '{}'. "
                        + "As the non-running shovel timeout of {} milliseconds has been reached, the shovel is now going to be deleted. "
                        + "Please check the RabbitMQ logs for more details. "
                        + "Shovel creation will be attempted later if the shovel is still required.",
                                 shovelName,
                                 timeObservedInNonRunningState,
                                 timeNow,
                                 retrievedShovel.getState().toString().toLowerCase(),
                                 nonRunningShovelTimeoutMilliseconds);                    

                    try {
                        shovelsApi.getApi().delete(rabbitMQVHost, shovelName);
                    } catch (final Exception e) {
                        final String errorMessage = String.format(
                                "Failed to delete shovel named %s. Will try again during the next run in %d " +
                                        "milliseconds (if the shovel is still present)",
                                shovelName,
                                nonRunningShovelTimeoutCheckIntervalMilliseconds);

                        LOGGER.error(errorMessage, e);

                        return;
                    }
                } else {
                    LOGGER.debug("Shovel named {} was observed in a non-running state at {}. The time now is {}. "
                        + "The shovel is not yet in a 'running' state. It's current state is '{}'. "
                        + "However, as the non-running shovel timeout of {} milliseconds has not been reached yet, "
                        + "the shovel will not be deleted at this time.",
                                 shovelName,
                                 timeObservedInNonRunningState,
                                 timeNow,
                                 retrievedShovel.getState().toString().toLowerCase(),
                                 nonRunningShovelTimeoutMilliseconds);
                }
            } 
        }
    }
}

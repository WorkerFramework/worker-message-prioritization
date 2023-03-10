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

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import net.jodah.expiringmap.ExpiringMap;

public final class ShovelRunningTooLongChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelRunningTooLongChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final LoadingCache<String,RabbitManagementApi<ShovelsApi>> nodeSpecificShovelsApiCache;
    private final Map<ShovelKey,Instant> shovelToTimeObservedInRunningStateMap;
    private final String rabbitMQVHost;
    private final String rabbitMQMgmtUrl;
    private final String rabbitMQMgmtUsername;
    private final String rabbitMQMgmtPassword;
    private final long shovelRunningTooLongTimeoutMilliseconds;
    private final long shovelRunningTooLongTimeoutCheckIntervalMilliseconds;

    public ShovelRunningTooLongChecker(
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final String rabbitMQVHost,
            final String rabbitMQMgmtUrl,
            final String rabbitMQMgmtUsername,
            final String rabbitMQMgmtPassword,
            final long shovelRunningTooLongTimeoutMilliseconds,
            final long shovelRunningTooLongTimeoutCheckIntervalMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.nodeSpecificShovelsApiCache = CacheBuilder.newBuilder()
                .build(new CacheLoader<String,RabbitManagementApi<ShovelsApi>>()
                {
                    @Override
                    public RabbitManagementApi<ShovelsApi> load(@Nonnull final String nodeSpecificRabbitMqMgmtUrl)
                    {
                        return new RabbitManagementApi<>(
                                ShovelsApi.class,
                                nodeSpecificRabbitMqMgmtUrl,
                                rabbitMQMgmtUsername,
                                rabbitMQMgmtPassword);
                    }
                });
        this.shovelToTimeObservedInRunningStateMap = ExpiringMap.builder().expiration(12, TimeUnit.HOURS).build();
        this.rabbitMQVHost = rabbitMQVHost;
        this.rabbitMQMgmtUrl = rabbitMQMgmtUrl;
        this.rabbitMQMgmtUsername = rabbitMQMgmtUsername;
        this.rabbitMQMgmtPassword = rabbitMQMgmtPassword;
        this.shovelRunningTooLongTimeoutMilliseconds = shovelRunningTooLongTimeoutMilliseconds;
        this.shovelRunningTooLongTimeoutCheckIntervalMilliseconds = shovelRunningTooLongTimeoutCheckIntervalMilliseconds;
    }

    @Override
    public void run()
    {
        final List<RetrievedShovel> retrievedShovels;
        try {
            retrievedShovels = shovelsApi.getApi().getShovels();
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels, so unable to check if any shovels are running too " +
                            "long and need to be restarted/recreated/deleted. Will try again during the next run in %d milliseconds",
                    shovelRunningTooLongTimeoutCheckIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }

        LOGGER.debug("Read the following list of shovels from the RabbitMQ API: {}", retrievedShovels);

        for (final RetrievedShovel retrievedShovel : retrievedShovels) {

            if (retrievedShovel.getState() == ShovelState.RUNNING) {

                final ShovelKey shovelKey = new ShovelKey(retrievedShovel.getName(), retrievedShovel.getTimestamp());

                final Instant timeObservedInRunningState = shovelToTimeObservedInRunningStateMap
                        .computeIfAbsent(shovelKey, s -> Instant.now());

                final long timeObservedInRunningStateMilliseconds = timeObservedInRunningState.toEpochMilli();

                final Instant timeNow = Instant.now();
                final long timeNowMilliseconds = timeNow.toEpochMilli();

                if (timeNowMilliseconds - timeObservedInRunningStateMilliseconds >= shovelRunningTooLongTimeoutMilliseconds) {

                    LOGGER.error("Shovel named {} has been observed in a 'running' state from {}. The time now is {}." +
                                    "As the 'shovel running too long timeout' of {} milliseconds has been reached, " +
                                    "we are now going to try to delete, restart, or recreate the shovel (in that order), as it is may " +
                                    "have become stuck. Shovel creation will be attempted later if deleting the shovel was " +
                                    "successful and the shovel is still required.",
                            shovelKey.getName(),
                            timeObservedInRunningState,
                            timeNow,
                            shovelRunningTooLongTimeoutMilliseconds);

                    final String nodeSpecificRabbitMqMgmtUrl =
                            NodeSpecificRabbitMqMgmtBuilder.getNodeSpecificRabbitMqMgmtUrl(retrievedShovel.getNode(), rabbitMQMgmtUrl);

                    final ShovelsApi nodeSpecificShovelsApi;
                    try {
                        nodeSpecificShovelsApi = nodeSpecificShovelsApiCache.get(nodeSpecificRabbitMqMgmtUrl).getApi();
                    } catch (final ExecutionException e) {
                        LOGGER.error(String.format("ExecutionException thrown while trying to get a node-specific ShovelsApi for %s",
                                nodeSpecificRabbitMqMgmtUrl), e);
                        return;
                    }

                    if (deleteShovel(retrievedShovel, nodeSpecificShovelsApi)) {
                        return;
                    }

                    if (restartShovel(retrievedShovel, nodeSpecificShovelsApi)) {
                        return;
                    }

                    if (!recreateShovel(retrievedShovel, nodeSpecificShovelsApi)) {
                        final String errorMessage = String.format(
                                "Shovel named %s is in a 'running' state and has a timestamp of %s. The time now is %s. " +
                                        "It has been observed in a 'running' state for %d milliseconds. " +
                                        "The 'shovel running too long timeout' of %d milliseconds has been reached, " +
                                        "meaning the shovel may have become stuck, " +
                                        "but attempts to delete, restart and recreate the shovel have failed.",
                                shovelKey.getName(),
                                shovelKey.getTimestamp(),
                                timeNow,
                                timeObservedInRunningStateMilliseconds,
                                shovelRunningTooLongTimeoutMilliseconds);

                        throw new RuntimeException(errorMessage);
                    }
                }
            }
        }
    }

    private boolean deleteShovel(final RetrievedShovel retrievedShovel, final ShovelsApi nodeSpecificShovelsApi)
    {
        try {
            nodeSpecificShovelsApi.delete(rabbitMQVHost, retrievedShovel.getName());

            LOGGER.info("Successfully deleted shovel named {} that had been running too long.", retrievedShovel.getName());

            return true;
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to delete shovel named %s that has been running too long.", retrievedShovel.getName());

            LOGGER.error(errorMessage, e);

            return false;
        }
    }

    private boolean restartShovel(final RetrievedShovel retrievedShovel, final ShovelsApi nodeSpecificShovelsApi)
    {
        try {
            nodeSpecificShovelsApi.restartShovel(rabbitMQVHost, retrievedShovel.getName());

            LOGGER.info("Successfully restarted shovel named {} that had been running too long.", retrievedShovel.getName());

            return true;
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to restart shovel named %s that has been running too long.", retrievedShovel.getName());

            LOGGER.error(errorMessage, e);

            return false;
        }
    }

    private boolean recreateShovel(final RetrievedShovel retrievedShovel, final ShovelsApi nodeSpecificShovelsApi)
    {
        final Shovel shovel = new Shovel();
        shovel.setSrcDeleteAfter(retrievedShovel.getSrcDeleteAfter());
        shovel.setAckMode(retrievedShovel.getAckMode());
        shovel.setSrcQueue(retrievedShovel.getSrcQueue());
        shovel.setSrcUri(retrievedShovel.getSrcUri());
        shovel.setDestQueue(retrievedShovel.getDestQueue());
        shovel.setDestUri(retrievedShovel.getDestUri());

        try {
            nodeSpecificShovelsApi.putShovel(rabbitMQVHost, retrievedShovel.getName(), new Component<>("shovel",
                    retrievedShovel.getName(),
                    retrievedShovel));

            LOGGER.info("Successfully recreated shovel named {} that had been running too long.", retrievedShovel.getName());

            return true;
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to recreate shovel named %s that has been running too long.", retrievedShovel.getName());

            LOGGER.error(errorMessage, e);

            return false;
        }
    }

    // A shovel can be created and deleted many times, each time it will have the same name.
    // As such, using the shovel name alone is not enough to uniquely identify a shovel, hence the use of the timestamp here as well.
    // Although the timestamp will change if a shovel is restarted (meaning a new ShovelKey will be created and inserted into the map),
    // that does not matter here for our purposes (the previous ShovelKey will not match any of the shovels retrieved from the RabbitMQ
    // API, and will eventually be expired from the map).
    final class ShovelKey
    {
        private final String name;
        private final Date timestamp;

        public ShovelKey(final String name, final Date timestamp)
        {
            this.name = name;
            this.timestamp = timestamp;
        }

        public String getName()
        {
            return name;
        }

        public Date getTimestamp()
        {
            return timestamp;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ShovelKey shovelKey = (ShovelKey)o;
            return Objects.equal(name, shovelKey.name) &&
                    Objects.equal(timestamp, shovelKey.timestamp);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(name, timestamp);
        }
    }
}

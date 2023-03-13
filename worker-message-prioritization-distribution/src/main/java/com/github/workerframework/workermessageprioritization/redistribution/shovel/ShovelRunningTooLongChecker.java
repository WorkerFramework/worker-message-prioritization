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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelState;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.base.Objects;
import com.google.common.cache.LoadingCache;

import net.jodah.expiringmap.ExpiringMap;

public final class ShovelRunningTooLongChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelRunningTooLongChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final LoadingCache<String, RabbitManagementApi<ShovelsApi>> nodeSpecificShovelsApiCache;
    private final Map<RunningShovelKey,Instant> shovelToTimeObservedInRunningStateMap;
    private final String rabbitMQVHost;
    private final long shovelRunningTooLongTimeoutMilliseconds;
    private final long shovelRunningTooLongTimeoutCheckIntervalMilliseconds;

    public ShovelRunningTooLongChecker(
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final LoadingCache<String, RabbitManagementApi<ShovelsApi>> nodeSpecificShovelsApiCache,
            final String rabbitMQVHost,
            final long shovelRunningTooLongTimeoutMilliseconds,
            final long shovelRunningTooLongTimeoutCheckIntervalMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.nodeSpecificShovelsApiCache = nodeSpecificShovelsApiCache;

        // Expire map entries a little after the timeout + interval between checks
        final long shovelToTimeObservedInRunningStateMapExpiryMilliseconds =
            (shovelRunningTooLongTimeoutMilliseconds + shovelRunningTooLongTimeoutCheckIntervalMilliseconds) * 2;

        this.shovelToTimeObservedInRunningStateMap = ExpiringMap
                .builder()
                .expiration(shovelToTimeObservedInRunningStateMapExpiryMilliseconds, TimeUnit.MILLISECONDS)
                .expirationListener((RunningShovelKey runningShovelKey, Instant timeObservedInRunningState) ->
                        LOGGER.debug("Expired entry from shovelToTimeObservedInRunningStateMap: {} -> {}",
                                runningShovelKey.getName(), timeObservedInRunningState))
                .build();

        this.rabbitMQVHost = rabbitMQVHost;
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

                final RunningShovelKey runningShovelKey = new RunningShovelKey(retrievedShovel.getName(), retrievedShovel.getTimestamp());

                final Instant timeObservedInRunningState = shovelToTimeObservedInRunningStateMap
                        .computeIfAbsent(runningShovelKey, s -> Instant.now());

                final long timeObservedInRunningStateMilliseconds = timeObservedInRunningState.toEpochMilli();

                final Instant timeNow = Instant.now();

                final boolean shovelRunningTooLong =
                        timeNow.toEpochMilli() - timeObservedInRunningStateMilliseconds >= shovelRunningTooLongTimeoutMilliseconds;

                if (shovelRunningTooLong) {

                    LOGGER.error("Shovel named {} has been observed in a 'running' state at {}. The time now is {}. " +
                                    "As the 'shovel running too long timeout' of {} milliseconds has been reached, " +
                                    "we are now going to try to repair the shovel by deleting, restarting, or recreating it " +
                                    "(in that order), as it is may have become stuck. Shovel creation will be attempted later if " +
                                    "deleting the shovel was successful and the shovel is still required.",
                            retrievedShovel.getName(),
                            timeObservedInRunningState,
                            timeNow,
                            shovelRunningTooLongTimeoutMilliseconds);

                    if (!ShovelRepairer.repairShovel(retrievedShovel, nodeSpecificShovelsApiCache, rabbitMQVHost)) {

                        LOGGER.error("Shovel named {} has been observed in a 'running' state at {}. The time now is {}. " +
                                        "The 'shovel running too long timeout' of {} milliseconds has been reached, " +
                                        "but attempts to repair the shovel by deleting, restarting, and recreating it " +
                                        "have failed. Will try again during the next run in {} milliseconds.",
                                retrievedShovel.getName(),
                                timeObservedInRunningState,
                                timeNow,
                                shovelRunningTooLongTimeoutMilliseconds,
                                shovelRunningTooLongTimeoutCheckIntervalMilliseconds);
                    }
                }
            }
        }
    }

    final class RunningShovelKey
    {
        private final String name;
        private final Date lastEnteredRunningStateTimestamp;

        public RunningShovelKey(final String name, final Date lastEnteredRunningStateTimestamp)
        {
            this.name = name;
            this.lastEnteredRunningStateTimestamp = lastEnteredRunningStateTimestamp;
        }

        public String getName()
        {
            return name;
        }

        public Date getLastEnteredRunningStateTimestamp()
        {
            return lastEnteredRunningStateTimestamp;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final RunningShovelKey shovelKey = (RunningShovelKey)o;
            return Objects.equal(name, shovelKey.name) &&
                    Objects.equal(lastEnteredRunningStateTimestamp, shovelKey.lastEnteredRunningStateTimestamp);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(name, lastEnteredRunningStateTimestamp);
        }
    }
}

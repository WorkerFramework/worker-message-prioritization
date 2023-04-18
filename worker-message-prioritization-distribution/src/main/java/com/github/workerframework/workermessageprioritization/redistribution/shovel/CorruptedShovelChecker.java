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

import static java.util.stream.Collectors.toSet;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelFromParametersApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.collect.Sets;

import net.jodah.expiringmap.ExpiringMap;

/**
 * Checks for shovels that are corrupted and deletes them.
 *
 * If an error occurs whilst creating a shovel, it is possible that the shovel has been created, but in a corrupt state where it is
 * not working correctly.
 *
 * For example, we have seen a shovel that appears on the RabbitMQ 'Shovel Management' UI (/api/parameters/shovel), but the shovel does
 * not appear on the RabbitMQ 'Shovel Status' UI (/api/shovels/). The shovel was not shovelling messages as it should have been, and
 * trying to recreate the shovel had no effect.
 *
 * So, for the purposes of this class, we are defining a 'corrupted' shovel as a shovel that is returned by /api/parameters/shovel but is
 * NOT returned by /api/shovels/.
 *
 * Note, because shovels returned by /api/parameters/shovel do not include 'state' or 'node' fields (those fields are only returned by
 * /api/shovels/), we cannot use the existing {@link NonRunningShovelChecker} or {@link ShovelRunningTooLongChecker} to check for corrupt
 * shovels, as these classes rely on the 'state' and 'node' fields, and corrupted shovels do not have a 'state' or a 'node'.
 */
public final class CorruptedShovelChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CorruptedShovelChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final String rabbitMQVHost;
    private final Map<String,Instant> shovelNameToTimeObservedCorrupted;
    private final long corruptedShovelTimeoutMilliseconds;
    private final long corruptedShovelCheckIntervalMilliseconds;

    public CorruptedShovelChecker(
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final String rabbitMQVHost,
            final long corruptedShovelTimeoutMilliseconds,
            final long corruptedShovelCheckIntervalMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.rabbitMQVHost = rabbitMQVHost;
        this.corruptedShovelTimeoutMilliseconds = corruptedShovelTimeoutMilliseconds;
        this.corruptedShovelCheckIntervalMilliseconds = corruptedShovelCheckIntervalMilliseconds;

        // Expire map entries a little after the timeout + interval between checks
        final long shovelNameToTimeObservedCorruptedExpiryMilliseconds =
                (corruptedShovelTimeoutMilliseconds + corruptedShovelCheckIntervalMilliseconds) * 2;

        this.shovelNameToTimeObservedCorrupted = ExpiringMap
                .builder()
                .expirationListener((String shoveName, Instant timeObservedCorrupted) ->
                        LOGGER.debug("Expired entry from shovelNameToTimeObservedCorrupted: {} -> {}",
                                shoveName, timeObservedCorrupted))
                .expiration(shovelNameToTimeObservedCorruptedExpiryMilliseconds, TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public void run()
    {
        // Get a list of shovels from /api/parameters/shovel
        final List<ShovelFromParametersApi> shovelsFromParametersApi;
        try {
            shovelsFromParametersApi = shovelsApi.getApi().getShovelsFromParametersApi(rabbitMQVHost);
            LOGGER.debug("Read {} shovels from /api/parameters/shovel: {}", shovelsFromParametersApi.size(), shovelsFromParametersApi);
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels from /api/parameters/shovel, so unable to check if any shovels are " +
                            "corrupted and need to be deleted. Will try again during the next run in %d milliseconds",
                    corruptedShovelCheckIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }

        // Get a list of shovels from /api/shovels/
        final List<RetrievedShovel> shovelsFromNonParametersApi;
        try {
            shovelsFromNonParametersApi = shovelsApi.getApi().getShovels();
            LOGGER.debug("Read {} shovels from /api/shovels/: {}", shovelsFromNonParametersApi.size(), shovelsFromNonParametersApi);
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels from /api/shovels/, so unable to check if any shovels are corrupted and " +
                            "need to be deleted. Will try again during the next run in %d milliseconds",
                    corruptedShovelCheckIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }

        // Find shovels that exist in /api/parameters/shovel but not in /api/shovels/, these are our corrupted shovels
        final Set<String> shovelsFromParametersApiNames = shovelsFromParametersApi
                .stream()
                .map(ShovelFromParametersApi::getName)
                .collect(toSet());

        final Set<String> shovelsFromNonParametersApiNames = shovelsFromNonParametersApi
                .stream()
                .filter(shovel -> shovel.getVhost().equals(rabbitMQVHost))
                .map(RetrievedShovel::getName)
                .collect(toSet());

        final Set<String> corruptedShovelNames = Sets.difference(shovelsFromParametersApiNames, shovelsFromNonParametersApiNames);

        // For each corrupted shovel, check if the corrupted shovel timeout has been reached, if so, delete the shovel
        for (final String corruptedShovelName : corruptedShovelNames) {

            final Instant timeObservedCorrupted = shovelNameToTimeObservedCorrupted
                    .computeIfAbsent(corruptedShovelName, s -> Instant.now());
            final long timeObservedCorruptedMilliseconds = timeObservedCorrupted.toEpochMilli();

            final Instant timeNow = Instant.now();

            final boolean corruptedShovelTimeoutReached =
                    timeNow.toEpochMilli() - timeObservedCorruptedMilliseconds >= corruptedShovelTimeoutMilliseconds;

            if (corruptedShovelTimeoutReached) {
                LOGGER.error("Found a shovel named {} that was returned by /api/parameters/shovel but not by /api/shovels/. " +
                             "This is an indication that the shovel may have become corrupted. " +
                                "The time when we first observed this corrupted shovel was {}. " +
                                "The time now is {}. " +
                                "As the corrupted shovel timeout of {} milliseconds has been reached, " +
                                "we are now going to try to delete the corrupted shovel. " +
                                "Shovel creation will be attempted later if the shovel is still required.",
                        corruptedShovelName,
                        timeObservedCorrupted,
                        timeNow,
                        corruptedShovelTimeoutMilliseconds);

                try {
                    shovelsApi.getApi().delete(rabbitMQVHost, corruptedShovelName);
                    LOGGER.info("Successfully deleted corrupted shovel named {}", corruptedShovelName);
                } catch (final Exception shovelsApiException) {
                    LOGGER.warn(String.format("Exception thrown during deletion of corrupted shovel named %s", corruptedShovelName),
                            shovelsApiException);
                }
            } else {
                LOGGER.debug("Found a shovel named {} that was returned by /api/parameters/shovel but not by /api/shovels/. " +
                             "This is an indication that the shovel may have become corrupted. " +
                                "The time when we first observed this corrupted shovel was {}. " +
                                "The time now is {}. " +
                                "As the corrupted shovel timeout of {} milliseconds has not yet been reached, " +
                                "the shovel will not be deleted at this time. ",
                        corruptedShovelName,
                        timeObservedCorrupted,
                        timeNow,
                        corruptedShovelTimeoutMilliseconds);
            }
        }
    }
}

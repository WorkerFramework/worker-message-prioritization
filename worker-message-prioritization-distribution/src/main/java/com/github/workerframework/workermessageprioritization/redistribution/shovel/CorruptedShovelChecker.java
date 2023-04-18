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

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelFromParametersApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.collect.Sets;

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
 * /api/shovels/), we cannot use the existing {@link NonRunningShovelChecker} or {@link NonRunningShovelChecker} to check for corrupt
 * shovels, as these classes rely on the 'state' and 'node' fields, and corrupted shovels do not have a 'state' or a 'node'.
 */
public final class CorruptedShovelChecker implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CorruptedShovelChecker.class);

    private final RabbitManagementApi<ShovelsApi> shovelsApi;
    private final String rabbitMQVHost;
    private final long corruptedShovelTimeoutCheckIntervalMilliseconds;

    public CorruptedShovelChecker(
            final RabbitManagementApi<ShovelsApi> shovelsApi,
            final String rabbitMQVHost,
            final long corruptedShovelTimeoutCheckIntervalMilliseconds)
    {
        this.shovelsApi = shovelsApi;
        this.rabbitMQVHost = rabbitMQVHost;
        this.corruptedShovelTimeoutCheckIntervalMilliseconds = corruptedShovelTimeoutCheckIntervalMilliseconds;
    }

    @Override
    public void run()
    {
        // Get a list of shovels from /api/parameters/shovel
        final List<ShovelFromParametersApi> shovelsFromParametersApi;
        try {
            shovelsFromParametersApi = shovelsApi.getApi().getShovelsFromParametersApi(rabbitMQVHost);
            LOGGER.debug("Read the following list of shovels from /api/parameters/shovel: {}", shovelsFromParametersApi);
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels from /api/parameters/shovel, so unable to check if any shovels are " +
                            "corrupted and need to be restarted/recreated/deleted. Will try again during the next run in %d milliseconds",
                    corruptedShovelTimeoutCheckIntervalMilliseconds);

            LOGGER.error(errorMessage, e);

            return;
        }

        // Get a list of shovels from /api/shovels/
        final List<RetrievedShovel> shovelsFromNonParametersApi;
        try {
            shovelsFromNonParametersApi = shovelsApi.getApi().getShovels();
            LOGGER.debug("Read the following list of shovels from /api/shovels/: {}", shovelsFromNonParametersApi);
        } catch (final Exception e) {
            final String errorMessage = String.format(
                    "Failed to get a list of existing shovels from /api/shovels/, so unable to check if any shovels are corrupted and " +
                            "need to be restarted/recreated/deleted. Will try again during the next run in %d milliseconds",
                    corruptedShovelTimeoutCheckIntervalMilliseconds);

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

        final Set<String> corruptedShovels = Sets.difference(shovelsFromParametersApiNames, shovelsFromNonParametersApiNames);

        // Delete each corrupted shovel
        for (final String corruptedShovel : corruptedShovels) {
            LOGGER.error("Found a shovel named {} that was returned by /api/parameters/shovel but not by /api/shovels/. " +
                    "This is an indication that the shovel may have become corrupted, so we are now going to try to delete the shovel. " +
                    "Shovel creation will be attempted later if deleting the shovel was successful and the shovel is still required.",
                    corruptedShovel);

            try {
                shovelsApi.getApi().delete(rabbitMQVHost, corruptedShovel);
                LOGGER.info("Successfully deleted corrupted shovel named {}", corruptedShovel);
            } catch (final Exception shovelsApiException) {
                LOGGER.warn(String.format("Exception thrown during deletion of corrupted shovel named %s", corruptedShovel),
                        shovelsApiException);
            }
        }
    }
}

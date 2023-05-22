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

import static com.github.workerframework.workermessageprioritization.redistribution.shovel.ShovelDistributor.ACK_MODE;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.Component;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RetrievedShovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.Shovel;
import com.github.workerframework.workermessageprioritization.rabbitmq.ShovelsApi;
import com.google.common.cache.LoadingCache;

final class ShovelRepairer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ShovelRepairer.class);

    private ShovelRepairer() {

    }

    public static boolean repairShovel(
            final RetrievedShovel retrievedShovel,
            final ShovelsApi shovelsApi,
            final LoadingCache<String,RabbitManagementApi<ShovelsApi>> nodeSpecificShovelsApiCache,
            final String rabbitMQVHost,
            final String rabbitMQUri)
    {
        final ShovelsApi nodeSpecificShovelsApi;
        try {
            nodeSpecificShovelsApi = nodeSpecificShovelsApiCache.get(retrievedShovel.getNode()).getApi();
        } catch (final ExecutionException e) {
            LOGGER.error(String.format("ExecutionException thrown while trying to get a node-specific ShovelsApi for %s",
                    retrievedShovel.getNode()), e);

            return false;
        }

        if (deleteShovel(retrievedShovel, shovelsApi, nodeSpecificShovelsApi, rabbitMQVHost)) {
            return true;
        }

        if (restartShovel(retrievedShovel, shovelsApi, nodeSpecificShovelsApi, rabbitMQVHost)) {
            return true;
        }

        if (recreateShovel(retrievedShovel, shovelsApi, nodeSpecificShovelsApi, rabbitMQVHost, rabbitMQUri)) {
            return true;
        }

        return false;
    }

    private static boolean deleteShovel(
            final RetrievedShovel retrievedShovel,
            final ShovelsApi shovelsApi,
            final ShovelsApi nodeSpecificShovelsApi,
            final String rabbitMQVHost)
    {
        try {
            nodeSpecificShovelsApi.delete(rabbitMQVHost, retrievedShovel.getName());

            LOGGER.info("Successfully deleted shovel named {}", retrievedShovel.getName());

            return true;
        } catch (final Exception nodeSpecificShovelsApiException) {
            LOGGER.error(String.format(
                    "Failed to delete shovel named %s using a node-specific Shovels API, will try using the general Shovels API...",
                    retrievedShovel.getName()), nodeSpecificShovelsApiException);

            try {
                shovelsApi.delete(rabbitMQVHost, retrievedShovel.getName());

                LOGGER.info("Successfully deleted shovel named {}", retrievedShovel.getName());

                return true;
            } catch (final Exception shovelsApiException) {
                LOGGER.error(String.format("Failed to delete shovel named %s", retrievedShovel.getName()), shovelsApiException);

                return false;
            }
        }
    }


    private static boolean restartShovel(
            final RetrievedShovel retrievedShovel,
            final ShovelsApi shovelsApi,
            final ShovelsApi nodeSpecificShovelsApi,
            final String rabbitMQVHost)
    {
        try {
            nodeSpecificShovelsApi.restartShovel(rabbitMQVHost, retrievedShovel.getName());

            LOGGER.info("Successfully restarted shovel named {}.", retrievedShovel.getName());

            return true;
        } catch (final Exception nodeSpecificShovelsApiException) {
            LOGGER.error(String.format(
                    "Failed to restart shovel named %s using a node-specific Shovels API, will try using the general Shovels API...",
                    retrievedShovel.getName()), nodeSpecificShovelsApiException);

            try {
                shovelsApi.restartShovel(rabbitMQVHost, retrievedShovel.getName());

                LOGGER.info("Successfully restarted shovel named {}.", retrievedShovel.getName());

                return true;
            } catch (final Exception shovelsApiException) {
                LOGGER.error(String.format("Failed to restart shovel named %s", retrievedShovel.getName()), shovelsApiException);

                return false;
            }
        }
    }

    private static boolean recreateShovel(
            final RetrievedShovel retrievedShovel,
            final ShovelsApi shovelsApi,
            final ShovelsApi nodeSpecificShovelsApi,
            final String rabbitMQVHost,
            final String rabbitMQUri
    )
    {
        // - Shovels fetched from the /api/shovels/{vhost} endpoint (the RetrievedShovel parameter) do NOT include the
        //   src-delete-after and ack-mode properties, which we need to recreate the shovel
        // - Shovels fetched from the /api/parameters/shovel/{vhost} endpoint DO include the src-delete-after and ack-mode properties,
        //   however, if a shovel gets stuck in a running state, the shovel may not be returned from the /api/parameters/shovel/{vhost}
        //   endpoint, so if we can't get the shovel from /api/parameters/shovel/{vhost}, we do not know what the src-delete-after or
        //   ack-mode properties should be
        // - Therefore, we are recreating the shovel with a src-delete-after of 0, so that the shovel gets recreated then immediately
        //   deleted. This allows the shovel to be created as normal (if still required) during the next loop of the application.

        final Shovel shovel = new Shovel();
        shovel.setSrcDeleteAfter(0);
        shovel.setAckMode(ACK_MODE);
        shovel.setSrcQueue(retrievedShovel.getSrcQueue());
        shovel.setSrcUri(rabbitMQUri); // Don't use retrievedShovel.getSrcUri() here, as it won't include the RabbitMQ username
        shovel.setDestQueue(retrievedShovel.getDestQueue());
        shovel.setDestUri(rabbitMQUri); // Don't use retrievedShovel.getDestUri() here, as it won't include the RabbitMQ username

        final String shovelName = retrievedShovel.getName();

        try {
            nodeSpecificShovelsApi.putShovel(rabbitMQVHost, shovelName, new Component<>("shovel", shovelName, shovel));

            LOGGER.info("Successfully recreated shovel named {} with properties {}", shovelName, shovel);

            return true;
        } catch (final Exception nodeSpecificShovelsApiException) {
            LOGGER.error(String.format(
                    "Failed to recreate shovel named %s with properties %s using a node-specific Shovels API, will try using the " +
                            "general Shovels API...",
                    shovelName, shovel), nodeSpecificShovelsApiException);

            try {
                shovelsApi.putShovel(rabbitMQVHost, shovelName, new Component<>("shovel", shovelName, shovel));

                LOGGER.info("Successfully recreated shovel named {} with properties {}", shovelName, shovel);

                return true;
            } catch (final Exception shovelsApiException) {
                LOGGER.error(String.format("Failed to recreate shovel named %s with properties %s", shovelName, shovel),
                        shovelsApiException);

                return false;
            }
        }
    }
}

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
package com.github.workerframework.workermessageprioritization.rerouting;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.HealthCheckApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.AlwaysRerouteDecider;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.RerouteDecider;
import com.google.common.base.Strings;
import com.hpe.caf.worker.document.model.Document;
import com.hpe.caf.worker.document.model.HealthMonitor;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MessageRouterSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouterSingleton.class);

    private static final boolean CAF_WMP_ENABLED = Boolean.parseBoolean(System.getenv("CAF_WMP_ENABLED"));

    private static Connection connection;
    private static RabbitManagementApi<HealthCheckApi> healthCheckApi;
    private static MessageRouter messageRouter;
    private static volatile boolean initAttempted = false;
    private static volatile String initError;

    public static void init() {

        if(messageRouter != null || initAttempted) {
            return;
        }

        initAttempted = true;

        try {

            final ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUsername(System.getenv("CAF_RABBITMQ_USERNAME"));
            connectionFactory.setPassword(System.getenv("CAF_RABBITMQ_PASSWORD"));
            connectionFactory.setHost(System.getenv("CAF_RABBITMQ_HOST"));
            connectionFactory.setPort(Integer.parseInt(System.getenv("CAF_RABBITMQ_PORT")));
            connectionFactory.setVirtualHost("/");

            final String mgmtEndpoint = System.getenv("CAF_RABBITMQ_MGMT_URL");
            final String mgmtUsername = System.getenv("CAF_RABBITMQ_MGMT_USERNAME");
            final String mgmtPassword = System.getenv("CAF_RABBITMQ_MGMT_PASSWORD");

            connection = connectionFactory.newConnection();

            final RabbitManagementApi<QueuesApi> queuesApi =
                    new RabbitManagementApi<>(QueuesApi.class, mgmtEndpoint, mgmtUsername, mgmtPassword);

            healthCheckApi = new RabbitManagementApi<>(HealthCheckApi.class, mgmtEndpoint, mgmtUsername, mgmtPassword);

            final String stagingQueueCacheExpiryMillisecondsString =
                    System.getenv("CAF_WMP_STAGING_QUEUE_CACHE_EXPIRY_MILLISECONDS");

            final long stagingQueueCacheExpiryMilliseconds =
                    !Strings.isNullOrEmpty(stagingQueueCacheExpiryMillisecondsString) ?
                            Long.parseLong(stagingQueueCacheExpiryMillisecondsString) :
                            60000;

            final StagingQueueCreator stagingQueueCreator = new StagingQueueCreator(
                    connectionFactory, queuesApi, stagingQueueCacheExpiryMilliseconds);

            final RerouteDecider rerouteDecider = new AlwaysRerouteDecider();

            LOGGER.debug("Using {} to decide whether to reroute messages", rerouteDecider.getClass().getSimpleName());

            messageRouter = new MessageRouter(queuesApi, "/", stagingQueueCreator, rerouteDecider);
        }
        catch (final Throwable e) {
            initError = String.format("Failed to initialise WMP - %s", e);
            LOGGER.error(initError);
            closeQuietly();
        }
    }

    public static void route(final Document document) {
        if(messageRouter != null) {
            messageRouter.route(document);
        }
    }

    public static String route(final String originalQueueName, final String tenantId) {
        if(messageRouter != null) {
            return messageRouter.route(originalQueueName, tenantId);
        } else {
            return originalQueueName;
        }
    }

    public static void checkHealth(final HealthMonitor healthMonitor) {

        if (CAF_WMP_ENABLED) {

            MessageRouterSingleton.init();

            if (initError != null) {
                healthMonitor.reportUnhealthy(initError);
            } else {
                try {
                    healthCheckApi.getApi().checkHealth();
                } catch (final Throwable t) {
                    final String errorMessage = String.format(
                            "RabbitMQ Management API healthcheck failed. Exception message: %s", t.getMessage());
                    healthMonitor.reportUnhealthy(errorMessage);
                    LOGGER.error(errorMessage, t);
                }
            }
        }
    }

    public static void close() throws IOException {
        if(connection != null) {
            connection.close();
        }
    }

    private static void closeQuietly() {
        try {
            close();
        } catch (final IOException iOException) {
            LOGGER.warn("IOException thrown trying to close connection", iOException);
        }
    }
}

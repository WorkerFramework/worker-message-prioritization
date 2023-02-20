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

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.QueuesApi;
import com.github.workerframework.workermessageprioritization.rabbitmq.RabbitManagementApi;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.AlwaysRerouteDecider;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.RerouteDecider;
import com.github.workerframework.workermessageprioritization.rerouting.reroutedeciders.TargetQueueCapacityRerouteDecider;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.K8sTargetQueueCapacityProvider;
import com.github.workerframework.workermessageprioritization.targetcapacitycalculators.TargetQueueCapacityProvider;
import com.google.common.base.Strings;
import com.hpe.caf.worker.document.model.Document;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class MessageRouterSingleton {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageRouterSingleton.class);

    private  static final String CAF_WMP_USE_TARGET_QUEUE_CAPACITY_TO_REROUTE = "CAF_WMP_USE_TARGET_QUEUE_CAPACITY_TO_REROUTE";

    private static final String CAF_WMP_KUBERNETES_NAMESPACES = "CAF_WMP_KUBERNETES_NAMESPACES";

    private static Connection connection;
    private static MessageRouter messageRouter;
    private static volatile boolean initAttempted = false;
    private static volatile boolean initShouldBeReattempted = false;

    public static void init() {

        if((messageRouter != null || initAttempted) && !initShouldBeReattempted) {
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

            final StagingQueueCreator stagingQueueCreator = new StagingQueueCreator(connection.createChannel());

            final RerouteDecider rerouteDecider;

            if (Boolean.parseBoolean(System.getenv(CAF_WMP_USE_TARGET_QUEUE_CAPACITY_TO_REROUTE))) {

                final List<String> kubernetesNamespaces =
                        Stream.of(Strings.nullToEmpty(System.getenv(CAF_WMP_KUBERNETES_NAMESPACES)).split(","))
                                .map(String::trim)
                                .filter(s -> !Strings.isNullOrEmpty(s))
                                .collect(toList());

                if (kubernetesNamespaces.isEmpty()) {
                    throw new RuntimeException(String.format(
                            "The %s environment variable should be provided when the %s environment variable is true. It should not be " +
                                    "null or empty. If multiple values are provided, they should be comma separated.",
                            CAF_WMP_KUBERNETES_NAMESPACES,
                            CAF_WMP_USE_TARGET_QUEUE_CAPACITY_TO_REROUTE));
                }

                final int kubernetesLabelCacheExpiryMinutes = Integer.parseInt(
                        Optional.ofNullable(System.getenv("CAF_WMP_KUBERNETES_LABEL_CACHE_EXPIRY_MINUTES")).orElse("60"));

                final TargetQueueCapacityProvider targetQueueCapacityProvider = new K8sTargetQueueCapacityProvider(
                        kubernetesNamespaces,
                        kubernetesLabelCacheExpiryMinutes);

                rerouteDecider = new TargetQueueCapacityRerouteDecider(targetQueueCapacityProvider);
            } else {
                rerouteDecider = new AlwaysRerouteDecider();
            }

            LOGGER.debug("Using {} to decide whether to reroute messages", rerouteDecider.getClass().getSimpleName());

            messageRouter = new MessageRouter(queuesApi, "/", stagingQueueCreator, rerouteDecider);

            initShouldBeReattempted = false;
        }
        catch (final Throwable e) {
            LOGGER.error("Failed to initialise WMP - {}", e.toString());
            closeQuietly();
        }

    }

    public static void route(final Document document) {
        if(messageRouter != null) {
            try {
                messageRouter.route(document);
            } catch (final Throwable throwable) {
                handleRouteFailure(throwable);
                throw throwable;
            }
        }
    }

    public static String route(final String originalQueueName, final String partitionId) {
        if(messageRouter != null) {
            try {
                return messageRouter.route(originalQueueName, partitionId);
            } catch (final Throwable throwable) {
                handleRouteFailure(throwable);
                throw throwable;
            }
        } else {
            return originalQueueName;
        }
    }

    private static void handleRouteFailure(final Throwable throwable) {
        LOGGER.error("Exception thrown trying to route document", throwable);

        // If an error has been thrown by the messageRouter.route call, it is possible that the connection and/or channel has
        // been closed, so we should attempt to init this class again the next time init() is called to create a new connection
        // and new channel.
        closeQuietly();

        initShouldBeReattempted = true;
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

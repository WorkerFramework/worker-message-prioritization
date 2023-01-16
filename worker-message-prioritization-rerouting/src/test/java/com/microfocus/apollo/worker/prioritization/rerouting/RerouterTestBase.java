/*
 * Copyright 2022-2022 Micro Focus or one of its affiliates.
 *
 * The only warranties for products and services of Micro Focus and its
 * affiliates and licensors ("Micro Focus") are set forth in the express
 * warranty statements accompanying such products and services. Nothing
 * herein should be construed as constituting an additional warranty.
 * Micro Focus shall not be liable for technical or editorial errors or
 * omissions contained herein. The information contained herein is subject
 * to change without notice.
 *
 * Contains Confidential Information. Except as specifically indicated
 * otherwise, a valid license is required for possession, use or copying.
 * Consistent with FAR 12.211 and 12.212, Commercial Computer Software,
 * Computer Software Documentation, and Technical Data for Commercial
 * Items are licensed to the U.S. Government under vendor's standard
 * commercial license.
 */
package com.microfocus.apollo.worker.prioritization.rerouting;

import com.microfocus.apollo.worker.prioritization.rabbitmq.QueuesApi;
import com.microfocus.apollo.worker.prioritization.rabbitmq.RabbitManagementApi;
import com.rabbitmq.client.ConnectionFactory;

import static com.microfocus.apollo.worker.prioritization.rerouting.MessageRouter.LOAD_BALANCED_INDICATOR;

public class RerouterTestBase {

    protected static final String TARGET_QUEUE_NAME = "target";
    protected static final String T1_STAGING_QUEUE_NAME = "tenant1";

    protected ConnectionFactory connectionFactory;
    protected RabbitManagementApi<QueuesApi> queuesApi;

    public RerouterTestBase() {

        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(System.getProperty("rabbitmq.node.address", "localhost"));
        connectionFactory.setUsername(System.getProperty("rabbitmq.username", "guest"));
        connectionFactory.setPassword(System.getProperty("rabbitmq.password", "guest"));
        connectionFactory.setPort(Integer.parseInt(System.getProperty("rabbitmq.node.port", "25672")));
        connectionFactory.setVirtualHost("/");

        final int managementPort = Integer.parseInt(System.getProperty("rabbitmq.ctrl.port", "25673"));

        queuesApi
            = new RabbitManagementApi<>(QueuesApi.class,
                                        "http://" + connectionFactory.getHost() + ":" + managementPort + "/",
                                        connectionFactory.getUsername(), connectionFactory.getPassword());
    }

    protected String getUniqueTargetQueueName(final String targetQueueName) {
        return targetQueueName + System.currentTimeMillis();
    }

    protected String getStagingQueueName(final String targetQueueName, final String stagingQueueName) {
        return targetQueueName + LOAD_BALANCED_INDICATOR + stagingQueueName;
    }
}

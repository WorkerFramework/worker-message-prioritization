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

import com.microfocus.apollo.worker.prioritization.rabbitmq.Queue;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public final class StagingQueueCreatorIT extends RerouterTestBase {

    @Test
    public void createStagingQueueTest() throws TimeoutException, IOException, InterruptedException {

        final var targetQueueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
        final var stagingQueueName = getStagingQueueName(targetQueueName, T1_STAGING_QUEUE_NAME);

        try (final var connection = connectionFactory.newConnection()) {

            try (final var channel = connection.createChannel()) {

                // Create a target queue
                final Map<String, Object> targetQueueArguments = Collections.singletonMap("x-max-priority", 5L);
                channel.queueDeclare(targetQueueName, true, false, false, targetQueueArguments);

                // Verify the target queue was created successfully
                final Queue targetQueue = queuesApi.getApi().getQueue("/", targetQueueName);
                Assert.assertNotNull("Target queue was not found via REST API", targetQueue);

                // Create a staging queue using the target queue as a template
                final StagingQueueCreator stagingQueueCreator = new StagingQueueCreator(channel);
                stagingQueueCreator.createStagingQueue(targetQueue, stagingQueueName);

                // Verify the staging queue was created successfully
                final Queue stagingQueue = queuesApi.getApi().getQueue("/", stagingQueueName);
                Assert.assertNotNull("Staging queue was not found via REST API", stagingQueue);
                Assert.assertEquals("Staging queue should have been created with the supplied name",
                                    stagingQueueName, stagingQueue.getName());
                Assert.assertEquals("Staging queue should have been created with the same arguments as the target queue",
                                    targetQueueArguments, stagingQueue.getArguments());
            }
        }
    }
}

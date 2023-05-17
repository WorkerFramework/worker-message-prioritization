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
package com.github.workerframework.workermessageprioritization.targetrefill;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;

import javax.annotation.Nonnull;

public final class K8sTargetQueueRefillProvider implements TargetQueueRefillProvider
{

    private static final Logger LOGGER = LoggerFactory.getLogger(K8sTargetQueueRefillProvider.class);
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL = "messageprioritization.targetqueuename";
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL = "messageprioritization.targetqueueeligibleforrefill";
    private static final long TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK = 50;
    private final List<String> kubernetesNamespaces;
    private final LoadingCache<Queue, Long> targetQueueEligibleForRefillCache;

    public K8sTargetQueueRefillProvider(final List<String> kubernetesNamespaces, final int kubernetesLabelCacheExpiryMinutes)
    {
        try {
            Configuration.setDefaultApiClient(ClientBuilder.standard().build());
        } catch (final IOException ioException) {
            throw new RuntimeException("IOException thrown trying to create a Kubernetes client", ioException);
        }

        this.targetQueueEligibleForRefillCache = CacheBuilder.newBuilder()
            .expireAfterWrite(kubernetesLabelCacheExpiryMinutes, TimeUnit.MINUTES)
            .build(new CacheLoader<Queue, Long>()
            {
                @Override
                public Long load(@Nonnull final Queue queue)
                {
                    return getTargetQueueEligibleForRefillFromKubernetes(queue);
                }
            });

        this.kubernetesNamespaces = kubernetesNamespaces;
    }

    @Override
    public long get(final Queue targetQueue)
    {
        try {
            return targetQueueEligibleForRefillCache.get(targetQueue);
        } catch (final ExecutionException executionException) {
            LOGGER.error(String.format("Cannot get refill threshold for the %s queue as an ExecutionException was thrown. "
                + "Falling back to using a refill threshold of %s", targetQueue, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK), executionException);

            return TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK;
        }
    }

    private long getTargetQueueEligibleForRefillFromKubernetes(final Queue targetQueue)
    {
        // Get the target queue name
        final String targetQueueName = targetQueue.getName();

        // Loop through all provided namespaces
        for (final String kubernetesNamespace : kubernetesNamespaces) {

            try {
                // Loop through all deployments (e.g. "elastic-worker", "elastic-query-worker")
                for (final V1Deployment deployment : Kubectl.get(V1Deployment.class).namespace(kubernetesNamespace).execute()) {

                    // Get the metadata
                    final V1ObjectMeta metadata = deployment.getMetadata();
                    if (metadata == null) {
                        continue;
                    }

                    // Get the labels from the metadata
                    final Map<String, String> labels = metadata.getLabels();
                    if (labels == null) {
                        continue;
                    }

                    // Check if there is a target queue name label
                    if (!labels.containsKey(MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL)) {
                        continue;
                    }

                    // Check if the target queue name label value matches the target queue name provided to this method
                    if (!labels.get(MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL).equals(targetQueueName)) {
                        continue;
                    }

                    // Check if there is a target queue eligible for refill label
                    if (!labels.containsKey(MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL)) {
                        // Throw RuntimeException as this indicates a deployment error that should never happen in production
                        throw new RuntimeException(String.format(
                            "Cannot get eligible for refill for the %s queue. The %s worker is missing the %s label",
                            targetQueueName, metadata.getName(), MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL));
                    }

                    // Return the value of the target queue eligible for refill label
                    final long targetQueueEligibleForRefill = Long.parseLong(labels.get(
                        MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL));

                    LOGGER.debug("Read the {} label belonging to {}. Setting the eligible for refill threshold of the {} queue to {}",
                                 MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL,
                                 metadata.getName(),
                                 targetQueueName,
                                 targetQueueEligibleForRefill);

                    return targetQueueEligibleForRefill;
                }
            } catch (final KubectlException kubectlException) {
                LOGGER.error(String.format(
                    "Cannot get eligible for refill threshold for the %s queue as the Kubernetes API threw an exception. "
                    + "Falling back to using a eligible for refill threshold of %s", targetQueueName,
                    TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK), kubectlException);

                return TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK;
            }
        }

        // Throw RuntimeException as this indicates a deployment error that should never happen in production
        throw new RuntimeException(String.format(
            "Cannot get eligible for refill threshold for the %s queue. Unable to find a worker with the %s label.",
            targetQueueName, MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL));
    }
}

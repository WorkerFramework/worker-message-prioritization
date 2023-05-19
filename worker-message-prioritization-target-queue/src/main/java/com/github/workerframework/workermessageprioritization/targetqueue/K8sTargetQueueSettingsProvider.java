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
package com.github.workerframework.workermessageprioritization.targetqueue;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public final class K8sTargetQueueSettingsProvider implements TargetQueueSettingsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(K8sTargetQueueSettingsProvider.class);
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL = "messageprioritization.targetqueuename";
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL = "messageprioritization.targetqueuemaxlength";
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL = "messageprioritization.targetqueueeligibleforrefill";
    private static final long TARGET_QUEUE_MAX_LENGTH_FALLBACK = 1000;
    private static final long TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK = 1;
    private final List<String> kubernetesNamespaces;
    private final LoadingCache<Queue, TargetQueueSettings> targetQueueToSettingsCache;

    public K8sTargetQueueSettingsProvider(final List<String> kubernetesNamespaces, final int kubernetesLabelCacheExpiryMinutes)
    {
        try {
            Configuration.setDefaultApiClient(ClientBuilder.standard().build());
        } catch (final IOException ioException) {
            throw new RuntimeException("IOException thrown trying to create a Kubernetes client", ioException);
        }

        this.targetQueueToSettingsCache = CacheBuilder.newBuilder()
            .expireAfterWrite(kubernetesLabelCacheExpiryMinutes, TimeUnit.MINUTES)
            .build(new CacheLoader<Queue, TargetQueueSettings>()
            {
                @Override
                public TargetQueueSettings load(@Nonnull final Queue queue)
                {
                    return getTargetQueueSettingsFromKubernetes(queue);
                }
            });

        this.kubernetesNamespaces = kubernetesNamespaces;
    }

    @Override
    public TargetQueueSettings get(final Queue targetQueue)
    {
        try {
            return targetQueueToSettingsCache.get(targetQueue);
        } catch (final ExecutionException executionException) {
            LOGGER.error(String.format("Cannot get settings for the %s queue as an ExecutionException was thrown. "
                + "Falling back to using max length of %s and eligible for refill threshold of %s",
                                       targetQueue, TARGET_QUEUE_MAX_LENGTH_FALLBACK, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK), executionException);

            return new TargetQueueSettings(TARGET_QUEUE_MAX_LENGTH_FALLBACK, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK);
        }
    }

    private TargetQueueSettings getTargetQueueSettingsFromKubernetes(final Queue targetQueue)
    {
        // Get the target queue name
        final String targetQueueName = targetQueue.getName();

        // Loop through all provided namespaces
        for (final String kubernetesNamespace : kubernetesNamespaces) {

            try {
                // Loop through all deployments
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

                    // Check if there is a target queue max length label
                    if (!labels.containsKey(MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL)) {
                        // Throw RuntimeException as this indicates a deployment error that should never happen in production
                        throw new RuntimeException(String.format(
                            "Cannot get max length for the %s queue. The %s worker is missing the %s label",
                            targetQueueName, metadata.getName(), MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL));
                    }

                    // Check if there is a target queue eligible for refill label
                    if (!labels.containsKey(MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL)) {
                        // Throw RuntimeException as this indicates a deployment error that should never happen in production
                        throw new RuntimeException(String.format(
                            "Cannot get eligible for refill for the %s queue. The %s worker is missing the %s label",
                            targetQueueName, metadata.getName(), MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL));
                    }

                    // Set the max length and eligible for refill settings
                    final long targetQueueMaxLength = Long.parseLong(labels.get(MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL));

                    final long targetQueueEligibleForRefill = Long.parseLong(labels.get(
                        MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL));

                    LOGGER.debug("Read the {} and {} labels belonging to {}. "
                        + "Setting max length to {} and eligible for refill to {} for the {} queue",
                                 MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL,
                                 MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_LABEL,
                                 metadata.getName(),
                                 targetQueueMaxLength,
                                 targetQueueEligibleForRefill,
                                 targetQueueName);

                    return new TargetQueueSettings(targetQueueMaxLength, targetQueueEligibleForRefill);
                }
            } catch (final KubectlException kubectlException) {
                LOGGER.error(String.format("Cannot get settings for the %s queue as the Kubernetes API threw an exception. "
                    + "Falling back to using a max length of %s and eligible for refill threshold of %s",
                                           targetQueueName, TARGET_QUEUE_MAX_LENGTH_FALLBACK, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK));

                return new TargetQueueSettings(TARGET_QUEUE_MAX_LENGTH_FALLBACK, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_FALLBACK);
            }
        }

        throw new RuntimeException(String.format(
            "Cannot get settings for the %s queue. Unable to find a worker with the required labels.",
            targetQueueName));
    }
}

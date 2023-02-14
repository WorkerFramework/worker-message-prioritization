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
package com.github.workerframework.workermessageprioritization.targetcapacitycalculators;

import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

/**
 * A {@link TargetQueueCapacityProvider} implementation that fetches the target queue capacity from a Kubernetes label.
 */
public final class K8sTargetQueueCapacityProvider implements TargetQueueCapacityProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(K8sTargetQueueCapacityProvider.class);

    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL = "messageprioritization.targetqueuename";

    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL = "messageprioritization.targetqueuemaxlength";

    private static final long FALLBACK_MAX_QUEUE_LENGTH = 1000;

    private final LoadingCache<Queue, Long> queueMaxLengthCache;

    private final List<String> namespaces;

    public static void main(String [] args) throws IOException
    {
        String kubeConfigPath = "C:\\Users\\RTorney\\Documents\\kube-config-larry"; // Copy from https://github.houston.softwaregrp.net/Verity/deploy/blob/master/override/kube-config-curly

        // https://github.com/kubernetes-client/java/blob/master/docs/kubectl-equivalence-in-java.md
        final ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(kubeConfigPath))).build();
        Configuration.setDefaultApiClient(client);

        final long l = new K8sTargetQueueCapacityProvider(Lists.newArrayList("private", "public")).get(null);
        System.out.println(l);
    }

    public K8sTargetQueueCapacityProvider(final List<String> namespaces)
    {
        this.queueMaxLengthCache = CacheBuilder.newBuilder()
                .expireAfterWrite(60, TimeUnit.MINUTES)
                .build(new CacheLoader<Queue,Long>()
                {
                    @Override
                    public Long load(@Nonnull final Queue queue) throws Exception
                    {
                        return getMaxQueueLengthFromKubernetes(queue);
                    }
                });

        this.namespaces = namespaces;
    }

    @Override
    public long get(final Queue targetQueue)
    {
        try {
            return queueMaxLengthCache.get(targetQueue);
        } catch (final ExecutionException executionException) {
            LOGGER.error(String.format("Cannot get max length for the %s queue as an ExecutionException was thrown. " +
                    "Falling back to using a max queue length of %s", targetQueue, FALLBACK_MAX_QUEUE_LENGTH), executionException);

            return FALLBACK_MAX_QUEUE_LENGTH;
        }
    }

    private long getMaxQueueLengthFromKubernetes(final Queue targetQueue)
    {
        // Get the target queue name
        final String targetQueueName = "dataprocessing-family-hashing-in";

        // Loop through all provided namespaces
        for (final String namespace : namespaces) {

            try {
                // Loop through all deployments (e.g. "elastic-worker", "elastic-query-worker")
                for (final V1Deployment deployment : Kubectl.get(V1Deployment.class).namespace(namespace).execute()) {

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
                        // RuntimeException as this indicates a deployment error that should never happen in production
                        throw new RuntimeException(String.format(
                                "Cannot get max length for the %s queue. The %s worker is missing the %s label",
                                targetQueueName, metadata.getName(), MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL));
                    }

                    // Return the value of the target queue max length label
                    final Long targetQueueMaxLength = Long.valueOf(labels.get(MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL));

                    LOGGER.debug("Setting the {} queue max length to {}", targetQueueName, targetQueueMaxLength);

                    return targetQueueMaxLength;
                }
            } catch (final KubectlException kubectlException) {
                LOGGER.error(String.format("Cannot get max length for the %s queue as the Kubernetes API threw an exception. " +
                        "Falling back to using a max queue length of %s", targetQueue, FALLBACK_MAX_QUEUE_LENGTH), kubectlException);

                return FALLBACK_MAX_QUEUE_LENGTH;
            }
        }

        // RuntimeException as this indicates a deployment error that should never happen in production
        throw new RuntimeException(String.format(
                "Cannot get max length for the %s queue. Unable to find a worker with the %s label.",
                targetQueueName, MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL));
    }
}

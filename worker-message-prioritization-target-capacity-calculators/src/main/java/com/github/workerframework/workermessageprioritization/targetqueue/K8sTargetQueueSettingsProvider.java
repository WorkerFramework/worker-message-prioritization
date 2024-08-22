/*
 * Copyright 2022-2024 Open Text.
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

import static com.github.workerframework.workermessageprioritization.targetqueue.Constants.*;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.restclients.KubernetesClientFactory;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.api.AppsV1Api;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.client.ApiClient;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.client.ApiException;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.model.IoK8sApimachineryPkgApisMetaV1ObjectMeta;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.model.IoK8sApiAppsV1Deployment;
import com.github.workerframework.workermessageprioritization.restclients.kubernetes.model.IoK8sApiAppsV1DeploymentSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class K8sTargetQueueSettingsProvider implements TargetQueueSettingsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(K8sTargetQueueSettingsProvider.class);

    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL = 
            "messageprioritization.targetqueuename";
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL = 
            "messageprioritization.targetqueuemaxlength";
    private static final String MESSAGE_PRIORITIZATION_MAX_INSTANCES_LABEL = "autoscale.maxinstances";
    private static final String MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_LABEL
        = "messageprioritization.targetqueueeligibleforrefillpercentage";
    private static final String CURRENT_INSTANCES_LABEL = "spec.replicas";
    private final List<String> kubernetesNamespaces;
    private final Supplier<Map<String, TargetQueueSettings>> memoizedTargetQueueSettingsSupplier;
    private final AppsV1Api appsV1Api;

    @Inject
    public K8sTargetQueueSettingsProvider(
            @Named("KubernetesNamespaces") final List<String> kubernetesNamespaces, 
            @Named("KubernetesLabelCacheExpiryMinutes") final int kubernetesLabelCacheExpiryMinutes)
    {
        try {
            final ApiClient clientWithCertAndToken = KubernetesClientFactory.createClientWithCertAndToken();
            this.appsV1Api = new AppsV1Api(clientWithCertAndToken);
        } catch (final Exception e) {
            throw new RuntimeException("Exception thrown trying to create a Kubernetes client", e);
        }

        this.kubernetesNamespaces = kubernetesNamespaces;
        this.memoizedTargetQueueSettingsSupplier = Suppliers.memoizeWithExpiration(
                this::getTargetQueueSettingsFromKubernetes, kubernetesLabelCacheExpiryMinutes, TimeUnit.MINUTES);
    }

    @Override
    public TargetQueueSettings get(final Queue targetQueue)
    {
        final TargetQueueSettings targetQueueSettings = memoizedTargetQueueSettingsSupplier.get().get(targetQueue.getName());

        if (targetQueueSettings == null) {
            LOGGER.warn("Cannot get settings for the {} queue. Using fallback settings: {}",
                    targetQueue.getName(),
                    FALLBACK_TARGET_QUEUE_SETTINGS);

            return FALLBACK_TARGET_QUEUE_SETTINGS;
        } else {
            LOGGER.debug("Got settings for the {} queue: {}", targetQueue.getName(), targetQueueSettings);

            return targetQueueSettings;
        }
    }

    private Map<String, TargetQueueSettings> getTargetQueueSettingsFromKubernetes()
    {
        // Map of target queue name -> settings
        final Map<String, TargetQueueSettings> targetQueueNameToSettingsMap = new HashMap<>();

        // Loop through all provided namespaces
        for (final String kubernetesNamespace : kubernetesNamespaces) {

            // Get all deployments in this namespace
            final List<IoK8sApiAppsV1Deployment> deployments;
            try {
                deployments = appsV1Api.listAppsV1NamespacedDeployment(
                        "private",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null).getItems();
            }  catch (final ApiException e) {
                LOGGER.error(String.format(
                        "Cannot get settings for the target queues in the %s namespace as the Kubernetes API threw an exception.",
                        kubernetesNamespace), e);

                // Try the next namespace
                continue;
            }

            // Loop through all deployments
            for (final IoK8sApiAppsV1Deployment deployment : deployments) {

                // Get the metadata
                final IoK8sApimachineryPkgApisMetaV1ObjectMeta metadata = deployment.getMetadata();
                if (metadata == null) {
                    continue;
                }

                // Get the spec
                final IoK8sApiAppsV1DeploymentSpec spec = deployment.getSpec();

                // Get the labels from the metadata
                final Map<String, String> labels = metadata.getLabels();
                if (labels == null) {
                    continue;
                }

                // Check if there is a target queue name label
                if (!labels.containsKey(MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL)) {
                    continue;
                }

                // Get the target queue name
                final String targetQueueName = labels.get(MESSAGE_PRIORITIZATION_TARGET_QUEUE_NAME_LABEL);

                final long targetQueueMaxLength = getLabelOrDefault(
                        labels, MESSAGE_PRIORITIZATION_TARGET_QUEUE_MAX_LENGTH_LABEL,
                        metadata.getName(), targetQueueName, TARGET_QUEUE_MAX_LENGTH_FALLBACK);

                long targetQueueEligibleForRefillPercentage = getLabelOrDefault(
                        labels, MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_LABEL,
                        metadata.getName(), targetQueueName, TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_FALLBACK);

                final long targetQueueMaxInstances = getLabelOrDefault(
                        labels, MESSAGE_PRIORITIZATION_MAX_INSTANCES_LABEL,
                        metadata.getName(), targetQueueName, MAX_INSTANCES_FALLBACK);

                final int currentInstances;
                if(spec.getReplicas() != null){
                    currentInstances = spec.getReplicas();
                }else{
                    // currentInstances not available for worker
                    LOGGER.debug(String.format("The worker %s is missing the %s label. ", metadata.getName(),
                            CURRENT_INSTANCES_LABEL));
                    currentInstances = CURRENT_INSTANCE_FALLBACK;
                }

                if (targetQueueEligibleForRefillPercentage < 0 || targetQueueEligibleForRefillPercentage > 100) {
                    // Invalid eligible for refill percentage provided, set to fall back value
                    LOGGER.warn(String.format("Cannot get eligible for refill percentage for the %s queue. "
                                    + "An invalid %s label was provided for the %s worker. "
                                    + "Falling back to using eligible for refill percentage of %s",
                            targetQueueName,
                            MESSAGE_PRIORITIZATION_TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_LABEL,
                            metadata.getName(), TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_FALLBACK));
                    targetQueueEligibleForRefillPercentage = TARGET_QUEUE_ELIGIBLE_FOR_REFILL_PERCENTAGE_FALLBACK;
                }

                final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(
                        targetQueueMaxLength,
                        targetQueueEligibleForRefillPercentage,
                        targetQueueMaxInstances,
                        currentInstances,
                        targetQueueMaxLength);

                LOGGER.debug("Adding entry to targetQueueNameToSettingsMap: {}={}", targetQueueName, targetQueueSettings);

                targetQueueNameToSettingsMap.put(targetQueueName, targetQueueSettings);
            }
        }

        return targetQueueNameToSettingsMap;
    }

    private static long getLabelOrDefault(
        final Map<String, String> labels,
        final String labelName,
        final String workerName,
        final String targetQueueName,
        final long defaultValue)
    {
        if (!labels.containsKey(labelName)) {
            LOGGER.debug(
                "Cannot get {} for the {} queue. The {} worker is missing the label. " +
                        "Falling back to using default value of {}",
                labelName, targetQueueName, workerName, defaultValue);

            return defaultValue;
        }

        final String labelValue = labels.get(labelName);
        try {
            return Long.parseLong(labelValue);
        } catch (final NumberFormatException ex) {
            LOGGER.error(
                    "Cannot get {} for the {} queue. " +
                    "The {} worker provided an invalid (not parsable to long) label value: {}. " +
                    "Falling back to using default value of {}",
                labelName, targetQueueName, workerName, labelValue, defaultValue);

            return defaultValue;
        }
    }
}

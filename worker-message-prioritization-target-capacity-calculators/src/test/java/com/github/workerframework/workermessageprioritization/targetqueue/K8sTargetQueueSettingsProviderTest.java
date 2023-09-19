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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.extended.kubectl.KubectlGet;
import io.kubernetes.client.extended.kubectl.exception.KubectlException;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1DeploymentSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;

public class K8sTargetQueueSettingsProviderTest
{
    @Test
    public void testExpectedSettingsAreProvidedWhenWorkerHasAllLabels() throws Exception
    {
        // Arrange
        final V1Deployment elasticQueryWorkerDeployment = createDeploymentWithLabels(
                "dataprocessing-elasticquery-worker",
                "private",
                "1",
                "3",
                "dataprocessing-elasticquery-in",
                "3000",
                "30",
                3);

        final V1Deployment appResourcesWorkerDeployment = createDeploymentWithLabels(
                "appresources-worker",
                "private",
                "1",
                "2",
                "appresources-worker-in",
                "10000",
                "20",
                2);

        final List<V1Deployment> deployments = Lists.newArrayList(elasticQueryWorkerDeployment, appResourcesWorkerDeployment);

        try (MockedStatic<ClientBuilder> clientBuilderStaticMock = Mockito.mockStatic(ClientBuilder.class);
             MockedStatic<Kubectl> kubectlStaticMock = Mockito.mockStatic(Kubectl.class)) {

            setupMocks(clientBuilderStaticMock, kubectlStaticMock, deployments);

            // Act
            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue elasticQueryWorkerQueue = new Queue();
            elasticQueryWorkerQueue.setName("dataprocessing-elasticquery-in");
            final TargetQueueSettings elasticQueryWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(elasticQueryWorkerQueue);

            final Queue appResourcesWorkerQueue = new Queue();
            appResourcesWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings appResourcesWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(appResourcesWorkerQueue);

            // Assert
            assertEquals("Unexpected value for current instances",
                    3, elasticQueryWorkerTargetQueueSettings.getCurrentInstances(), 0.0);
            assertEquals("Unexpected value for max instances",
                    3, elasticQueryWorkerTargetQueueSettings.getMaxInstances(), 0.0);
            assertEquals("Unexpected value for current max length",
                    3000, elasticQueryWorkerTargetQueueSettings.getCurrentMaxLength());
            assertEquals("Unexpected value for eligible for refill percentage",
                    30, elasticQueryWorkerTargetQueueSettings.getEligibleForRefillPercentage());
            assertEquals("Unexpected value for capacity",
                    3000, elasticQueryWorkerTargetQueueSettings.getCapacity());

            assertEquals("Unexpected value for current instances",
                    2, appResourcesWorkerTargetQueueSettings.getCurrentInstances(), 0.0);
            assertEquals("Unexpected value for max instances",
                    2, appResourcesWorkerTargetQueueSettings.getMaxInstances(), 0.0);
            assertEquals("Unexpected value for current max length",
                    10000, appResourcesWorkerTargetQueueSettings.getCurrentMaxLength());
            assertEquals("Unexpected value for eligible for refill percentage",
                    20, appResourcesWorkerTargetQueueSettings.getEligibleForRefillPercentage());
            assertEquals("Unexpected value for capacity",
                    10000, appResourcesWorkerTargetQueueSettings.getCapacity());
        }
    }

    @Test
    public void testFallbackSettingsAreProvidedWhenWorkerIsMissingLabels() throws KubectlException
    {
        // Arrange
        final V1Deployment appResourcesWorkerDeployment = createDeploymentWithoutLabels(
                "appresources-worker",
                "private",
                2);

        final List<V1Deployment> deployments = Lists.newArrayList(appResourcesWorkerDeployment);

        try (MockedStatic<ClientBuilder> clientBuilderStaticMock = Mockito.mockStatic(ClientBuilder.class);
             MockedStatic<Kubectl> kubectlStaticMock = Mockito.mockStatic(Kubectl.class)) {

            setupMocks(clientBuilderStaticMock, kubectlStaticMock, deployments);

            // Act
            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue appResourcesWorkerQueue = new Queue();
            appResourcesWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings appResourcesWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(appResourcesWorkerQueue);

            // Assert
            assertEquals("Unexpected value for current instances",
                    1, appResourcesWorkerTargetQueueSettings.getCurrentInstances(), 0.0);
            assertEquals("Unexpected value for max instances",
                    1, appResourcesWorkerTargetQueueSettings.getMaxInstances(), 0.0);
            assertEquals("Unexpected value for current max length",
                    1000, appResourcesWorkerTargetQueueSettings.getCurrentMaxLength());
            assertEquals("Unexpected value for eligible for refill percentage",
                    10, appResourcesWorkerTargetQueueSettings.getEligibleForRefillPercentage());
            assertEquals("Unexpected value for capacity",
                    1000, appResourcesWorkerTargetQueueSettings.getCapacity());
        }
    }

    @Test
    public void testFallbackSettingsAreProvidedWhenWorkerIsMissingFromDeployment() throws KubectlException
    {
        // Arrange
        final List<V1Deployment> deployments = Collections.emptyList();

        try (MockedStatic<ClientBuilder> clientBuilderStaticMock = Mockito.mockStatic(ClientBuilder.class);
             MockedStatic<Kubectl> kubectlStaticMock = Mockito.mockStatic(Kubectl.class)) {

            setupMocks(clientBuilderStaticMock, kubectlStaticMock, deployments);

            // Act
            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue elasticQueryWorkerQueue = new Queue();
            elasticQueryWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings elasticQueryWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(elasticQueryWorkerQueue);

            // Assert
            assertEquals("Unexpected value for current instances",
                    1, elasticQueryWorkerTargetQueueSettings.getCurrentInstances(), 0.0);
            assertEquals("Unexpected value for max instances",
                    1, elasticQueryWorkerTargetQueueSettings.getMaxInstances(), 0.0);
            assertEquals("Unexpected value for current max length",
                    1000, elasticQueryWorkerTargetQueueSettings.getCurrentMaxLength());
            assertEquals("Unexpected value for eligible for refill percentage",
                    10, elasticQueryWorkerTargetQueueSettings.getEligibleForRefillPercentage());
            assertEquals("Unexpected value for capacity",
                    1000, elasticQueryWorkerTargetQueueSettings.getCapacity());
        }
    }

    private static V1Deployment createDeploymentWithLabels(
            final String name,
            final String namespace,
            final String minInstances,
            final String maxInstances,
            final String targetQueueName,
            final String targetQueueMaxLength,
            final String targetQueueEligibleForRefillPercentage,
            final int replicas)
    {
        final V1Deployment deployment = new V1Deployment();

        deployment.setMetadata(new V1ObjectMeta());
        deployment.getMetadata().setName(name);
        deployment.getMetadata().setNamespace(namespace);
        deployment.getMetadata().setLabels(ImmutableMap.<String,String>builder()
                .put("autoscale.mininstances", minInstances)
                .put("autoscale.maxinstances", maxInstances)
                .put("messageprioritization.targetqueuename", targetQueueName)
                .put("messageprioritization.targetqueuemaxlength", targetQueueMaxLength)
                .put("messageprioritization.targetqueueeligibleforrefillpercentage", targetQueueEligibleForRefillPercentage)
                .build());
        deployment.setSpec(new V1DeploymentSpec());
        deployment.getSpec().setReplicas(replicas);

        return deployment;
    }

    private static V1Deployment createDeploymentWithoutLabels(
            final String name,
            final String namespace,
            final int replicas)
    {
        final V1Deployment deployment = new V1Deployment();

        deployment.setMetadata(new V1ObjectMeta());
        deployment.getMetadata().setName(name);
        deployment.getMetadata().setNamespace(namespace);
        deployment.setSpec(new V1DeploymentSpec());
        deployment.getSpec().setReplicas(replicas);

        return deployment;
    }

    private static void setupMocks(
            final MockedStatic<ClientBuilder> clientBuilderStaticMock,
            final MockedStatic<Kubectl> kubectlStaticMock,
            final List<V1Deployment> deployments) throws KubectlException
    {
        final ClientBuilder clientBuilderMock = mock(ClientBuilder.class);
        clientBuilderStaticMock.when(() -> ClientBuilder.standard()).thenReturn(clientBuilderMock);
        when(clientBuilderMock.build()).thenReturn(null);

        final KubectlGet<V1Deployment> getMock = mock(KubectlGet.class);
        kubectlStaticMock.when(() -> Kubectl.get(V1Deployment.class)).thenReturn(getMock);
        final KubectlGet<V1Deployment> namespaceMock = mock(KubectlGet.class);
        when(getMock.namespace("private")).thenReturn(namespaceMock);
        when(namespaceMock.execute()).thenReturn(deployments);
    }
}

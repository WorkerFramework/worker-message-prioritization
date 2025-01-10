/*
 * Copyright 2022-2025 Open Text.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.cafapi.kubernetes.client.KubernetesClientFactory;
import com.github.cafapi.kubernetes.client.api.AppsV1Api;
import com.github.cafapi.kubernetes.client.client.ApiClient;
import com.github.cafapi.kubernetes.client.model.IoK8sApiAppsV1Deployment;
import com.github.cafapi.kubernetes.client.model.IoK8sApiAppsV1DeploymentList;
import com.github.cafapi.kubernetes.client.model.IoK8sApiAppsV1DeploymentSpec;
import com.github.cafapi.kubernetes.client.model.IoK8sApimachineryPkgApisMetaV1ObjectMeta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class K8sTargetQueueSettingsProviderTest
{
    @Test
    public void testExpectedSettingsAreProvidedWhenWorkerHasAllLabels() throws Exception
    {
        // Arrange
        final IoK8sApiAppsV1Deployment elasticQueryWorkerDeployment = createDeploymentWithLabels(
                "dataprocessing-elasticquery-worker",
                "private",
                "1",
                "3",
                "dataprocessing-elasticquery-in",
                "3000",
                "30",
                3);

        final IoK8sApiAppsV1Deployment appResourcesWorkerDeployment = createDeploymentWithLabels(
                "appresources-worker",
                "private",
                "1",
                "2",
                "appresources-worker-in",
                "10000",
                "20",
                2);

        final IoK8sApiAppsV1DeploymentList deploymentList = new IoK8sApiAppsV1DeploymentList();
        deploymentList.setItems(Lists.newArrayList(elasticQueryWorkerDeployment, appResourcesWorkerDeployment));

        final AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest apilistAppsV1NamespacedDeploymentRequestMock =
                mock(AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest.class);

        when(apilistAppsV1NamespacedDeploymentRequestMock.execute()).thenReturn(deploymentList);

        try (MockedStatic<KubernetesClientFactory> clientFactoryStaticMock = Mockito.mockStatic(KubernetesClientFactory.class);
             MockedConstruction<AppsV1Api> mockedAppsV1Api = Mockito.mockConstruction(AppsV1Api.class, (mock, context) -> {

                 when(mock.listAppsV1NamespacedDeployment("private")).thenReturn(apilistAppsV1NamespacedDeploymentRequestMock);
             })
        ) {
            final ApiClient apiClientMock = mock(ApiClient.class);
            clientFactoryStaticMock.when(KubernetesClientFactory::createClientWithCertAndToken).thenReturn(apiClientMock);

            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue elasticQueryWorkerQueue = new Queue();
            elasticQueryWorkerQueue.setName("dataprocessing-elasticquery-in");
            final TargetQueueSettings elasticQueryWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(elasticQueryWorkerQueue);

            final Queue appResourcesWorkerQueue = new Queue();
            appResourcesWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings appResourcesWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get(appResourcesWorkerQueue);

            // Assert
            assertEquals(3, elasticQueryWorkerTargetQueueSettings.getCurrentInstances(), 0.0,
                    "Unexpected value for current instances");
            assertEquals(3, elasticQueryWorkerTargetQueueSettings.getMaxInstances(), 0.0,
                    "Unexpected value for max instances");
            assertEquals(3000, elasticQueryWorkerTargetQueueSettings.getCurrentMaxLength(),
                    "Unexpected value for current max length");
            assertEquals(30, elasticQueryWorkerTargetQueueSettings.getEligibleForRefillPercentage(),
                    "Unexpected value for eligible for refill percentage");
            assertEquals(3000, elasticQueryWorkerTargetQueueSettings.getCapacity(),
                    "Unexpected value for capacity");

            assertEquals(2, appResourcesWorkerTargetQueueSettings.getCurrentInstances(), 0.0,
                    "Unexpected value for current instances");
            assertEquals(2, appResourcesWorkerTargetQueueSettings.getMaxInstances(), 0.0,
                    "Unexpected value for max instances");
            assertEquals(10000, appResourcesWorkerTargetQueueSettings.getCurrentMaxLength(),
                    "Unexpected value for current max length");
            assertEquals(20, appResourcesWorkerTargetQueueSettings.getEligibleForRefillPercentage(),
                    "Unexpected value for eligible for refill percentage");
            assertEquals(10000, appResourcesWorkerTargetQueueSettings.getCapacity(),
                    "Unexpected value for capacity");
        }
    }


    @Test
    public void testFallbackSettingsAreProvidedWhenWorkerIsMissingLabels() throws Exception
    {
        // Arrange
        final IoK8sApiAppsV1Deployment appResourcesWorkerDeployment = createDeploymentWithoutLabels(
                "appresources-worker",
                "private",
                2);

        final IoK8sApiAppsV1DeploymentList deploymentList = new IoK8sApiAppsV1DeploymentList();
        deploymentList.setItems(Lists.newArrayList(appResourcesWorkerDeployment, appResourcesWorkerDeployment));

        final AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest apilistAppsV1NamespacedDeploymentRequestMock =
                mock(AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest.class);

        when(apilistAppsV1NamespacedDeploymentRequestMock.execute()).thenReturn(deploymentList);

        try (MockedStatic<KubernetesClientFactory> clientFactoryStaticMock = Mockito.mockStatic(KubernetesClientFactory.class);
             MockedConstruction<AppsV1Api> mockedAppsV1Api = Mockito.mockConstruction(AppsV1Api.class, (mock, context) -> {

                 when(mock.listAppsV1NamespacedDeployment("private")).thenReturn(apilistAppsV1NamespacedDeploymentRequestMock);
             })
        ) {
            final ApiClient apiClientMock = mock(ApiClient.class);
            clientFactoryStaticMock.when(KubernetesClientFactory::createClientWithCertAndToken).thenReturn(apiClientMock);

            // Act
            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue appResourcesWorkerQueue = new Queue();
            appResourcesWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings appResourcesWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get
                    (appResourcesWorkerQueue);

            // Assert
            assertEquals(1, appResourcesWorkerTargetQueueSettings.getCurrentInstances(), 0.0,
                    "Unexpected value for current instances");
            assertEquals(1, appResourcesWorkerTargetQueueSettings.getMaxInstances(), 0.0,
                    "Unexpected value for max instances");
            assertEquals(1000, appResourcesWorkerTargetQueueSettings.getCurrentMaxLength(),
                    "Unexpected value for current max length");
            assertEquals(10, appResourcesWorkerTargetQueueSettings.getEligibleForRefillPercentage(),
                    "Unexpected value for eligible for refill percentage");
            assertEquals(1000, appResourcesWorkerTargetQueueSettings.getCapacity(),
                    "Unexpected value for capacity");
        }
    }

    @Test
    public void testFallbackSettingsAreProvidedWhenWorkerIsMissingFromDeployment() throws Exception
    {
        // Arrange
        final IoK8sApiAppsV1DeploymentList deploymentList = new IoK8sApiAppsV1DeploymentList();
        deploymentList.setItems(Collections.emptyList());

        final AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest apilistAppsV1NamespacedDeploymentRequestMock =
                mock(AppsV1Api.APIlistAppsV1NamespacedDeploymentRequest.class);

        when(apilistAppsV1NamespacedDeploymentRequestMock.execute()).thenReturn(deploymentList);

        try (MockedStatic<KubernetesClientFactory> clientFactoryStaticMock = Mockito.mockStatic(KubernetesClientFactory.class);
             MockedConstruction<AppsV1Api> mockedAppsV1Api = Mockito.mockConstruction(AppsV1Api.class, (mock, context) -> {

                 when(mock.listAppsV1NamespacedDeployment("private")).thenReturn(apilistAppsV1NamespacedDeploymentRequestMock);
             })
        ) {
            final ApiClient apiClientMock = mock(ApiClient.class);
            clientFactoryStaticMock.when(KubernetesClientFactory::createClientWithCertAndToken).thenReturn(apiClientMock);

            // Act
            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider =
                    new K8sTargetQueueSettingsProvider(Lists.newArrayList("private"), 60);

            final Queue elasticQueryWorkerQueue = new Queue();
            elasticQueryWorkerQueue.setName("appresources-worker-in");
            final TargetQueueSettings elasticQueryWorkerTargetQueueSettings = k8sTargetQueueSettingsProvider.get
                    (elasticQueryWorkerQueue);

            // Assert
            assertEquals(1, elasticQueryWorkerTargetQueueSettings.getCurrentInstances(), 0.0,
                    "Unexpected value for current instances");
            assertEquals(1, elasticQueryWorkerTargetQueueSettings.getMaxInstances(), 0.0,
                    "Unexpected value for max instances");
            assertEquals(1000, elasticQueryWorkerTargetQueueSettings.getCurrentMaxLength(),
                    "Unexpected value for current max length");
            assertEquals(10, elasticQueryWorkerTargetQueueSettings.getEligibleForRefillPercentage(),
                    "Unexpected value for eligible for refill percentage");
            assertEquals(1000, elasticQueryWorkerTargetQueueSettings.getCapacity(),
                    "Unexpected value for capacity");
        }
    }

    private static IoK8sApiAppsV1Deployment createDeploymentWithLabels(
            final String name,
            final String namespace,
            final String minInstances,
            final String maxInstances,
            final String targetQueueName,
            final String targetQueueMaxLength,
            final String targetQueueEligibleForRefillPercentage,
            final int replicas)
    {
        final IoK8sApiAppsV1Deployment deployment = new IoK8sApiAppsV1Deployment();

        deployment.setMetadata(new IoK8sApimachineryPkgApisMetaV1ObjectMeta());
        deployment.getMetadata().setName(name);
        deployment.getMetadata().setNamespace(namespace);
        deployment.getMetadata().setLabels(ImmutableMap.<String,String>builder()
                .put("autoscale.mininstances", minInstances)
                .put("autoscale.maxinstances", maxInstances)
                .put("messageprioritization.targetqueuename", targetQueueName)
                .put("messageprioritization.targetqueuemaxlength", targetQueueMaxLength)
                .put("messageprioritization.targetqueueeligibleforrefillpercentage", targetQueueEligibleForRefillPercentage)
                .build());
        deployment.setSpec(new IoK8sApiAppsV1DeploymentSpec());
        deployment.getSpec().setReplicas(replicas);

        return deployment;
    }

    private static IoK8sApiAppsV1Deployment createDeploymentWithoutLabels(
            final String name,
            final String namespace,
            final int replicas)
    {
        final IoK8sApiAppsV1Deployment deployment = new IoK8sApiAppsV1Deployment();

        deployment.setMetadata(new IoK8sApimachineryPkgApisMetaV1ObjectMeta());
        deployment.getMetadata().setName(name);
        deployment.getMetadata().setNamespace(namespace);
        deployment.setSpec(new IoK8sApiAppsV1DeploymentSpec());
        deployment.getSpec().setReplicas(replicas);

        return deployment;
    }
}

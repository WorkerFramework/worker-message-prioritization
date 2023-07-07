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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.ConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.consumption.EqualConsumptionTargetCalculator;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.LowLevelDistributor;
import com.github.workerframework.workermessageprioritization.redistribution.lowlevel.StagingTargetPairProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import io.kubernetes.client.extended.kubectl.Kubectl;
import io.kubernetes.client.openapi.models.V1Deployment;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.internal.MockWebServerExtension;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockWebServerExtension.class)
public class TunedTargetQueueIT extends DistributorTestBase {

    final String queueName = getUniqueTargetQueueName(TARGET_QUEUE_NAME);
    final String stagingQueue1Name = getStagingQueueName(queueName, T1_STAGING_QUEUE_NAME);
    final String stagingQueue2Name = getStagingQueueName(queueName, T2_STAGING_QUEUE_NAME);

    // This test is for development purposes only
    // This test is to observe the consumption rate altering the recommended target queue length.
    // Console outputs and debugger should be used to see these dynamic changes.
    @Test
    public void tunedTargetQueueNoOpModeTest(final MockWebServer mockK8sServer) throws TimeoutException, IOException {
        try (final Connection connection = connectionFactory.newConnection()) {

//            final MockWebServer mockK8sServer = new MockWebServer();
            final int queueSize = 500000;
            final int sleepTime = 10000;
            final int roundingMultiple = 100;
            final int maxConsumptionRateHistorySize = 100;
            final int minConsumptionRateHistorySize = 100;
            final long currentTargetQueueLength = 1000;
            final long eligibleForRefillPercent = 10;
            final double maxInstances = 4;
            final double currentInstances = 2;
            final int queueProcessingTimeGoalSeconds = 300;

            final boolean noOpMode = !Strings.isNullOrEmpty(System.getenv("CAF_NOOP_MODE")) ?
                    Boolean.parseBoolean(System.getenv("CAF_NOOP_MODE")) : true;

            Channel channel = connection.createChannel();

            channel.queueDeclare(queueName, false, false, false, null);
            channel.queueDeclare(stagingQueue1Name, true, false, false, Collections.emptyMap());
            channel.queueDeclare(stagingQueue2Name, true, false, false, Collections.emptyMap());

            Assert.assertNotNull("Queue was not found via REST API", queuesApi.getApi().getQueue("/", queueName));
            Assert.assertNotNull("Staging queue was not found via REST API", queuesApi.getApi().getQueue("/", stagingQueue1Name));
            Assert.assertNotNull("Staging queue was not found via REST API", queuesApi.getApi().getQueue("/", stagingQueue2Name));

            final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            final String body = gson.toJson(new Object());

            channel.basicPublish("", stagingQueue1Name, properties, body.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish("", stagingQueue2Name, properties, body.getBytes(StandardCharsets.UTF_8));

            // Verify the target queue was created successfully
            final Queue targetQueue = queuesApi.getApi().getQueue("/", queueName);
            Assert.assertNotNull("Target queue was not found via REST API", targetQueue);

            String message = "Hello World!";
            IntStream.range(1, queueSize).forEach(i -> {
                try {
                    channel.basicPublish("", queueName, null, message.getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                new String(delivery.getBody(), "UTF-8");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

            // Sleep to allow time for the messages to start being consumed.
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody("{\n" +
                            "\t\"kind\": \"Deployment\",\n" +
                            "\t\"apiVersion\": \"apps/v1\",\n" +
                            "\t\"metadata\": {\n" +
                            "\t\t\"name\": \"archivecleanup-worker\",\n" +
                            "\t\t\"namespace\": \"private\",\n" +
                            "\t\t\"uid\": \"9aba7256-d21b-4bb6-9119-d4c3f7200665\",\n" +
                            "\t\t\"resourceVersion\": \"103405334\",\n" +
                            "\t\t\"generation\": 1,\n" +
                            "\t\t\"creationTimestamp\": \"2023-07-06T11:41:41Z\",\n" +
                            "\t\t\"labels\": {\n" +
                            "\t\t\t\"autoscale.backoff\": \"10\",\n" +
                            "\t\t\t\"autoscale.groupid\": \"managed-queue-workers\",\n" +
                            "\t\t\t\"autoscale.interval\": \"30\",\n" +
                            "\t\t\t\"autoscale.maxinstances\": \"4\",\n" +
                            "\t\t\t\"autoscale.metric\": \"rabbitmq\",\n" +
                            "\t\t\t\"autoscale.mininstances\": \"1\",\n" +
                            "\t\t\t\"autoscale.scalingprofile\": \"default\",\n" +
                            "\t\t\t\"autoscale.scalingtarget\": \"dataprocessing-archive-cleanup-in\",\n" +
                            "\t\t\t\"autoscale.shutdownpriority\": \"5\",\n" +
                            "\t\t\t\"fas-object-type\": \"application\",\n" +
                            "\t\t\t\"messageprioritization.targetqueueeligibleforrefillpercentage\": \"10\",\n" +
                            "\t\t\t\"messageprioritization.targetqueuemaxlength\": \"1000\",\n" +
                            "\t\t\t\"messageprioritization.targetqueuename\": \"dataprocessing-archive-cleanup-in\"\n" +
                            "\t\t},\n" +
                            "\t\t\"annotations\": {\n" +
                            "\t\t\t\"deployment.kubernetes.io/revision\": \"1\",\n" +
                            "\t\t\t\"docker.image.build.date\": \"2023-05-22T10:11:26.759154757Z\",\n" +
                            "\t\t\t\"docker.image.build.number\": \"2.2.0-SNAPSHOT\",\n" +
                            "\t\t\t\"docker.image.git.branch\": \"master\",\n" +
                            "\t\t\t\"docker.image.git.commit\": \"75119f4e7d9f087e4231beb199babd740198e62f\",\n" +
                            "\t\t\t\"docker.image.git.repo\": \"https://github.houston.softwaregrp.net/Verity/worker-archive-cleanup\",\n" +
                            "\t\t\t\"generated-by\": \"deploy-tools:4.1.0-789\",\n" +
                            "\t\t\t\"kubectl.kubernetes.io/last-applied-configuration\": \"{\\\"apiVersion\\\":\\\"apps/v1\\\",\\\"kind\\\":\\\"Deployment\\\",\\\"metadata\\\":{\\\"annotations\\\":{\\\"docker.image.build.date\\\":\\\"2023-05-22T10:11:26.759154757Z\\\",\\\"docker.image.build.number\\\":\\\"2.2.0-SNAPSHOT\\\",\\\"docker.image.git.branch\\\":\\\"master\\\",\\\"docker.image.git.commit\\\":\\\"75119f4e7d9f087e4231beb199babd740198e62f\\\",\\\"docker.image.git.repo\\\":\\\"https://github.houston.softwaregrp.net/Verity/worker-archive-cleanup\\\",\\\"generated-by\\\":\\\"deploy-tools:4.1.0-789\\\"},\\\"labels\\\":{\\\"autoscale.backoff\\\":\\\"10\\\",\\\"autoscale.groupid\\\":\\\"managed-queue-workers\\\",\\\"autoscale.interval\\\":\\\"30\\\",\\\"autoscale.maxinstances\\\":\\\"4\\\",\\\"autoscale.metric\\\":\\\"rabbitmq\\\",\\\"autoscale.mininstances\\\":\\\"1\\\",\\\"autoscale.scalingprofile\\\":\\\"default\\\",\\\"autoscale.scalingtarget\\\":\\\"dataprocessing-archive-cleanup-in\\\",\\\"autoscale.shutdownpriority\\\":\\\"5\\\",\\\"fas-object-type\\\":\\\"application\\\",\\\"messageprioritization.targetqueueeligibleforrefillpercentage\\\":\\\"10\\\",\\\"messageprioritization.targetqueuemaxlength\\\":\\\"1000\\\",\\\"messageprioritization.targetqueuename\\\":\\\"dataprocessing-archive-cleanup-in\\\"},\\\"name\\\":\\\"archivecleanup-worker\\\",\\\"namespace\\\":\\\"private\\\"},\\\"spec\\\":{\\\"replicas\\\":1,\\\"selector\\\":{\\\"matchLabels\\\":{\\\"app\\\":\\\"archivecleanup-worker\\\"}},\\\"strategy\\\":{\\\"type\\\":\\\"Recreate\\\"},\\\"template\\\":{\\\"metadata\\\":{\\\"annotations\\\":{\\\"vault.hashicorp.com/agent-inject\\\":\\\"true\\\",\\\"vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_MGMT_PASSWORD\\\":\\\"secret/marathon/shared/CAF_RABBITMQ_MGMT_PASSWORD\\\",\\\"vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_PASSWORD\\\":\\\"secret/marathon/shared/CAF_RABBITMQ_PASSWORD\\\",\\\"vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_MGMT_PASSWORD\\\":\\\"{{- with secret \\\\\\\"secret/marathon/shared/CAF_RABBITMQ_MGMT_PASSWORD\\\\\\\" -}}{{ .Data.value }}{{- end -}}\\\",\\\"vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_PASSWORD\\\":\\\"{{- with secret \\\\\\\"secret/marathon/shared/CAF_RABBITMQ_PASSWORD\\\\\\\" -}}{{ .Data.value }}{{- end -}}\\\",\\\"vault.hashicorp.com/agent-pre-populate-only\\\":\\\"true\\\",\\\"vault.hashicorp.com/ca-cert\\\":\\\"/vault/tls/ca-bundle.crt\\\",\\\"vault.hashicorp.com/role\\\":\\\"fas\\\",\\\"vault.hashicorp.com/tls-secret\\\":\\\"vault-tls-secret\\\"},\\\"labels\\\":{\\\"app\\\":\\\"archivecleanup-worker\\\"},\\\"namespace\\\":\\\"private\\\"},\\\"spec\\\":{\\\"containers\\\":[{\\\"envFrom\\\":[{\\\"configMapRef\\\":{\\\"name\\\":\\\"archivecleanup-worker-config\\\"}}],\\\"image\\\":\\\"saas-docker-prerelease.svsartifactory.swinfra.net/sepg/builds/verity/worker-archive-cleanup/master/ci/134/worker-archive-cleanup:2.2.0-SNAPSHOT\\\",\\\"imagePullPolicy\\\":\\\"Always\\\",\\\"livenessProbe\\\":{\\\"failureThreshold\\\":5,\\\"httpGet\\\":{\\\"path\\\":\\\"/healthcheck\\\",\\\"port\\\":8081,\\\"scheme\\\":\\\"HTTP\\\"},\\\"initialDelaySeconds\\\":15,\\\"periodSeconds\\\":120,\\\"timeoutSeconds\\\":20},\\\"name\\\":\\\"archivecleanup-worker\\\",\\\"readinessProbe\\\":{\\\"failureThreshold\\\":5,\\\"httpGet\\\":{\\\"path\\\":\\\"/healthcheck\\\",\\\"port\\\":8081,\\\"scheme\\\":\\\"HTTP\\\"},\\\"initialDelaySeconds\\\":15,\\\"periodSeconds\\\":120,\\\"timeoutSeconds\\\":20},\\\"resources\\\":{\\\"limits\\\":{\\\"memory\\\":\\\"1184Mi\\\"},\\\"requests\\\":{\\\"cpu\\\":\\\"400.0m\\\",\\\"memory\\\":\\\"592Mi\\\"}},\\\"startupProbe\\\":{\\\"failureThreshold\\\":8,\\\"httpGet\\\":{\\\"path\\\":\\\"/healthcheck\\\",\\\"port\\\":8081,\\\"scheme\\\":\\\"HTTP\\\"},\\\"initialDelaySeconds\\\":15,\\\"periodSeconds\\\":120,\\\"timeoutSeconds\\\":20},\\\"volumeMounts\\\":[{\\\"mountPath\\\":\\\"/mnt/orchestrator/sandbox\\\",\\\"name\\\":\\\"certificates\\\",\\\"readOnly\\\":true},{\\\"mountPath\\\":\\\"/etc/store\\\",\\\"name\\\":\\\"nfs-caf-storage\\\"}]}],\\\"enableServiceLinks\\\":false,\\\"imagePullSecrets\\\":[{\\\"name\\\":\\\"registrypullsecret\\\"}],\\\"serviceAccount\\\":\\\"fas\\\",\\\"tolerations\\\":[{\\\"effect\\\":\\\"NoSchedule\\\",\\\"key\\\":\\\"apollo-worker-type\\\",\\\"operator\\\":\\\"Equal\\\",\\\"value\\\":\\\"private\\\"}],\\\"volumes\\\":[{\\\"name\\\":\\\"certificates\\\",\\\"secret\\\":{\\\"items\\\":[{\\\"key\\\":\\\"cicada_CA_renew20220113.pem\\\",\\\"path\\\":\\\"cicada_CA_renew20220113.pem\\\"},{\\\"key\\\":\\\"MFI_INTERMEDIATE_CA_CERT.pem\\\",\\\"path\\\":\\\"MFI_INTERMEDIATE_CA_CERT.pem\\\"},{\\\"key\\\":\\\"MFI_ISSUING_CA_CERT.pem\\\",\\\"path\\\":\\\"MFI_ISSUING_CA_CERT.pem\\\"},{\\\"key\\\":\\\"MFI_ROOT_CERT.pem\\\",\\\"path\\\":\\\"MFI_ROOT_CERT.pem\\\"}],\\\"secretName\\\":\\\"fas-certificates\\\"}},{\\\"name\\\":\\\"nfs-caf-storage\\\",\\\"persistentVolumeClaim\\\":{\\\"claimName\\\":\\\"nfs-caf-storage\\\",\\\"readOnly\\\":false}}]}}}}\\n\"\n" +
                            "\t\t},\n" +
                            "\t\t\"managedFields\": [\n" +
                            "\t\t\t{\n" +
                            "\t\t\t\t\"manager\": \"kubectl-client-side-apply\",\n" +
                            "\t\t\t\t\"operation\": \"Update\",\n" +
                            "\t\t\t\t\"apiVersion\": \"apps/v1\",\n" +
                            "\t\t\t\t\"time\": \"2023-07-06T11:41:41Z\",\n" +
                            "\t\t\t\t\"fieldsType\": \"FieldsV1\",\n" +
                            "\t\t\t\t\"fieldsV1\": {\n" +
                            "\t\t\t\t\t\"f:metadata\": {\n" +
                            "\t\t\t\t\t\t\"f:annotations\": {\n" +
                            "\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:docker.image.build.date\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:docker.image.build.number\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:docker.image.git.branch\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:docker.image.git.commit\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:docker.image.git.repo\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:generated-by\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:kubectl.kubernetes.io/last-applied-configuration\": {}\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"f:labels\": {\n" +
                            "\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.backoff\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.groupid\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.interval\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.maxinstances\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.metric\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.mininstances\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.scalingprofile\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.scalingtarget\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:autoscale.shutdownpriority\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:fas-object-type\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:messageprioritization.targetqueueeligibleforrefillpercentage\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:messageprioritization.targetqueuemaxlength\": {},\n" +
                            "\t\t\t\t\t\t\t\"f:messageprioritization.targetqueuename\": {}\n" +
                            "\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\"f:spec\": {\n" +
                            "\t\t\t\t\t\t\"f:progressDeadlineSeconds\": {},\n" +
                            "\t\t\t\t\t\t\"f:replicas\": {},\n" +
                            "\t\t\t\t\t\t\"f:revisionHistoryLimit\": {},\n" +
                            "\t\t\t\t\t\t\"f:selector\": {},\n" +
                            "\t\t\t\t\t\t\"f:strategy\": {\n" +
                            "\t\t\t\t\t\t\t\"f:type\": {}\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"f:template\": {\n" +
                            "\t\t\t\t\t\t\t\"f:metadata\": {\n" +
                            "\t\t\t\t\t\t\t\t\"f:annotations\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-inject\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_MGMT_PASSWORD\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_PASSWORD\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_MGMT_PASSWORD\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_PASSWORD\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/agent-pre-populate-only\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/ca-cert\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/role\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:vault.hashicorp.com/tls-secret\": {}\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\"f:labels\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"f:app\": {}\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\"f:namespace\": {}\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"f:spec\": {\n" +
                            "\t\t\t\t\t\t\t\t\"f:containers\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\"k:{\\\"name\\\":\\\"archivecleanup-worker\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:envFrom\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:image\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:imagePullPolicy\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:livenessProbe\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:failureThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:path\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:port\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:scheme\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:initialDelaySeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:periodSeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:successThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:timeoutSeconds\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:name\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:readinessProbe\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:failureThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:path\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:port\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:scheme\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:initialDelaySeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:periodSeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:successThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:timeoutSeconds\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:resources\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:limits\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:memory\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:requests\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:cpu\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:memory\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:startupProbe\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:failureThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:path\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:port\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:scheme\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:initialDelaySeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:periodSeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:successThreshold\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:timeoutSeconds\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:terminationMessagePath\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:terminationMessagePolicy\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:volumeMounts\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"k:{\\\"mountPath\\\":\\\"/etc/store\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:mountPath\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:name\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"k:{\\\"mountPath\\\":\\\"/mnt/orchestrator/sandbox\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:mountPath\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:name\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\t\"f:readOnly\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\"f:dnsPolicy\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:enableServiceLinks\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:imagePullSecrets\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"k:{\\\"name\\\":\\\"registrypullsecret\\\"}\": {}\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\"f:restartPolicy\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:schedulerName\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:securityContext\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:serviceAccount\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:serviceAccountName\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:terminationGracePeriodSeconds\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:tolerations\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:volumes\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\"k:{\\\"name\\\":\\\"certificates\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:name\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:secret\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:defaultMode\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:items\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:secretName\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t\t\"k:{\\\"name\\\":\\\"nfs-caf-storage\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:name\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\"f:persistentVolumeClaim\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\t\t\t\"f:claimName\": {}\n" +
                            "\t\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t}\n" +
                            "\t\t\t},\n" +
                            "\t\t\t{\n" +
                            "\t\t\t\t\"manager\": \"kube-controller-manager\",\n" +
                            "\t\t\t\t\"operation\": \"Update\",\n" +
                            "\t\t\t\t\"apiVersion\": \"apps/v1\",\n" +
                            "\t\t\t\t\"time\": \"2023-07-07T05:37:41Z\",\n" +
                            "\t\t\t\t\"fieldsType\": \"FieldsV1\",\n" +
                            "\t\t\t\t\"fieldsV1\": {\n" +
                            "\t\t\t\t\t\"f:metadata\": {\n" +
                            "\t\t\t\t\t\t\"f:annotations\": {\n" +
                            "\t\t\t\t\t\t\t\"f:deployment.kubernetes.io/revision\": {}\n" +
                            "\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\"f:status\": {\n" +
                            "\t\t\t\t\t\t\"f:availableReplicas\": {},\n" +
                            "\t\t\t\t\t\t\"f:conditions\": {\n" +
                            "\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\"k:{\\\"type\\\":\\\"Available\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:lastTransitionTime\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:lastUpdateTime\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:message\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:reason\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:status\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:type\": {}\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"k:{\\\"type\\\":\\\"Progressing\\\"}\": {\n" +
                            "\t\t\t\t\t\t\t\t\".\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:lastTransitionTime\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:lastUpdateTime\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:message\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:reason\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:status\": {},\n" +
                            "\t\t\t\t\t\t\t\t\"f:type\": {}\n" +
                            "\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"f:observedGeneration\": {},\n" +
                            "\t\t\t\t\t\t\"f:readyReplicas\": {},\n" +
                            "\t\t\t\t\t\t\"f:replicas\": {},\n" +
                            "\t\t\t\t\t\t\"f:updatedReplicas\": {}\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t},\n" +
                            "\t\t\t\t\"subresource\": \"status\"\n" +
                            "\t\t\t}\n" +
                            "\t\t]\n" +
                            "\t},\n" +
                            "\t\"spec\": {\n" +
                            "\t\t\"replicas\": 1,\n" +
                            "\t\t\"selector\": {\n" +
                            "\t\t\t\"matchLabels\": {\n" +
                            "\t\t\t\t\"app\": \"archivecleanup-worker\"\n" +
                            "\t\t\t}\n" +
                            "\t\t},\n" +
                            "\t\t\"template\": {\n" +
                            "\t\t\t\"metadata\": {\n" +
                            "\t\t\t\t\"namespace\": \"private\",\n" +
                            "\t\t\t\t\"creationTimestamp\": null,\n" +
                            "\t\t\t\t\"labels\": {\n" +
                            "\t\t\t\t\t\"app\": \"archivecleanup-worker\"\n" +
                            "\t\t\t\t},\n" +
                            "\t\t\t\t\"annotations\": {\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-inject\": \"true\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_MGMT_PASSWORD\": \"secret/marathon/shared/CAF_RABBITMQ_MGMT_PASSWORD\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-inject-secret-CAF_RABBITMQ_PASSWORD\": \"secret/marathon/shared/CAF_RABBITMQ_PASSWORD\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_MGMT_PASSWORD\": \"{{- with secret \\\"secret/marathon/shared/CAF_RABBITMQ_MGMT_PASSWORD\\\" -}}{{ .Data.value }}{{- end -}}\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-inject-template-CAF_RABBITMQ_PASSWORD\": \"{{- with secret \\\"secret/marathon/shared/CAF_RABBITMQ_PASSWORD\\\" -}}{{ .Data.value }}{{- end -}}\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/agent-pre-populate-only\": \"true\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/ca-cert\": \"/vault/tls/ca-bundle.crt\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/role\": \"fas\",\n" +
                            "\t\t\t\t\t\"vault.hashicorp.com/tls-secret\": \"vault-tls-secret\"\n" +
                            "\t\t\t\t}\n" +
                            "\t\t\t},\n" +
                            "\t\t\t\"spec\": {\n" +
                            "\t\t\t\t\"volumes\": [\n" +
                            "\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\"name\": \"certificates\",\n" +
                            "\t\t\t\t\t\t\"secret\": {\n" +
                            "\t\t\t\t\t\t\t\"secretName\": \"fas-certificates\",\n" +
                            "\t\t\t\t\t\t\t\"items\": [\n" +
                            "\t\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\t\"key\": \"cicada_CA_renew20220113.pem\",\n" +
                            "\t\t\t\t\t\t\t\t\t\"path\": \"cicada_CA_renew20220113.pem\"\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\t\"key\": \"MFI_INTERMEDIATE_CA_CERT.pem\",\n" +
                            "\t\t\t\t\t\t\t\t\t\"path\": \"MFI_INTERMEDIATE_CA_CERT.pem\"\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\t\"key\": \"MFI_ISSUING_CA_CERT.pem\",\n" +
                            "\t\t\t\t\t\t\t\t\t\"path\": \"MFI_ISSUING_CA_CERT.pem\"\n" +
                            "\t\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\t\"key\": \"MFI_ROOT_CERT.pem\",\n" +
                            "\t\t\t\t\t\t\t\t\t\"path\": \"MFI_ROOT_CERT.pem\"\n" +
                            "\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t],\n" +
                            "\t\t\t\t\t\t\t\"defaultMode\": 420\n" +
                            "\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t},\n" +
                            "\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\"name\": \"nfs-caf-storage\",\n" +
                            "\t\t\t\t\t\t\"persistentVolumeClaim\": {\n" +
                            "\t\t\t\t\t\t\t\"claimName\": \"nfs-caf-storage\"\n" +
                            "\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t],\n" +
                            "\t\t\t\t\"containers\": [\n" +
                            "\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\"name\": \"archivecleanup-worker\",\n" +
                            "\t\t\t\t\t\t\"image\": \"saas-docker-prerelease.svsartifactory.swinfra.net/sepg/builds/verity/worker-archive-cleanup/master/ci/134/worker-archive-cleanup:2.2.0-SNAPSHOT\",\n" +
                            "\t\t\t\t\t\t\"envFrom\": [\n" +
                            "\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\"configMapRef\": {\n" +
                            "\t\t\t\t\t\t\t\t\t\"name\": \"archivecleanup-worker-config\"\n" +
                            "\t\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t],\n" +
                            "\t\t\t\t\t\t\"resources\": {\n" +
                            "\t\t\t\t\t\t\t\"limits\": {\n" +
                            "\t\t\t\t\t\t\t\t\"memory\": \"1184Mi\"\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"requests\": {\n" +
                            "\t\t\t\t\t\t\t\t\"cpu\": \"400m\",\n" +
                            "\t\t\t\t\t\t\t\t\"memory\": \"592Mi\"\n" +
                            "\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"volumeMounts\": [\n" +
                            "\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\"name\": \"certificates\",\n" +
                            "\t\t\t\t\t\t\t\t\"readOnly\": true,\n" +
                            "\t\t\t\t\t\t\t\t\"mountPath\": \"/mnt/orchestrator/sandbox\"\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\t\t\"name\": \"nfs-caf-storage\",\n" +
                            "\t\t\t\t\t\t\t\t\"mountPath\": \"/etc/store\"\n" +
                            "\t\t\t\t\t\t\t}\n" +
                            "\t\t\t\t\t\t],\n" +
                            "\t\t\t\t\t\t\"livenessProbe\": {\n" +
                            "\t\t\t\t\t\t\t\"httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\"path\": \"/healthcheck\",\n" +
                            "\t\t\t\t\t\t\t\t\"port\": 8081,\n" +
                            "\t\t\t\t\t\t\t\t\"scheme\": \"HTTP\"\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"initialDelaySeconds\": 15,\n" +
                            "\t\t\t\t\t\t\t\"timeoutSeconds\": 20,\n" +
                            "\t\t\t\t\t\t\t\"periodSeconds\": 120,\n" +
                            "\t\t\t\t\t\t\t\"successThreshold\": 1,\n" +
                            "\t\t\t\t\t\t\t\"failureThreshold\": 5\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"readinessProbe\": {\n" +
                            "\t\t\t\t\t\t\t\"httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\"path\": \"/healthcheck\",\n" +
                            "\t\t\t\t\t\t\t\t\"port\": 8081,\n" +
                            "\t\t\t\t\t\t\t\t\"scheme\": \"HTTP\"\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"initialDelaySeconds\": 15,\n" +
                            "\t\t\t\t\t\t\t\"timeoutSeconds\": 20,\n" +
                            "\t\t\t\t\t\t\t\"periodSeconds\": 120,\n" +
                            "\t\t\t\t\t\t\t\"successThreshold\": 1,\n" +
                            "\t\t\t\t\t\t\t\"failureThreshold\": 5\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"startupProbe\": {\n" +
                            "\t\t\t\t\t\t\t\"httpGet\": {\n" +
                            "\t\t\t\t\t\t\t\t\"path\": \"/healthcheck\",\n" +
                            "\t\t\t\t\t\t\t\t\"port\": 8081,\n" +
                            "\t\t\t\t\t\t\t\t\"scheme\": \"HTTP\"\n" +
                            "\t\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\t\"initialDelaySeconds\": 15,\n" +
                            "\t\t\t\t\t\t\t\"timeoutSeconds\": 20,\n" +
                            "\t\t\t\t\t\t\t\"periodSeconds\": 120,\n" +
                            "\t\t\t\t\t\t\t\"successThreshold\": 1,\n" +
                            "\t\t\t\t\t\t\t\"failureThreshold\": 8\n" +
                            "\t\t\t\t\t\t},\n" +
                            "\t\t\t\t\t\t\"terminationMessagePath\": \"/dev/termination-log\",\n" +
                            "\t\t\t\t\t\t\"terminationMessagePolicy\": \"File\",\n" +
                            "\t\t\t\t\t\t\"imagePullPolicy\": \"Always\"\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t],\n" +
                            "\t\t\t\t\"restartPolicy\": \"Always\",\n" +
                            "\t\t\t\t\"terminationGracePeriodSeconds\": 30,\n" +
                            "\t\t\t\t\"dnsPolicy\": \"ClusterFirst\",\n" +
                            "\t\t\t\t\"serviceAccountName\": \"fas\",\n" +
                            "\t\t\t\t\"serviceAccount\": \"fas\",\n" +
                            "\t\t\t\t\"securityContext\": {},\n" +
                            "\t\t\t\t\"imagePullSecrets\": [\n" +
                            "\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\"name\": \"registrypullsecret\"\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t],\n" +
                            "\t\t\t\t\"schedulerName\": \"default-scheduler\",\n" +
                            "\t\t\t\t\"tolerations\": [\n" +
                            "\t\t\t\t\t{\n" +
                            "\t\t\t\t\t\t\"key\": \"apollo-worker-type\",\n" +
                            "\t\t\t\t\t\t\"operator\": \"Equal\",\n" +
                            "\t\t\t\t\t\t\"value\": \"private\",\n" +
                            "\t\t\t\t\t\t\"effect\": \"NoSchedule\"\n" +
                            "\t\t\t\t\t}\n" +
                            "\t\t\t\t],\n" +
                            "\t\t\t\t\"enableServiceLinks\": false\n" +
                            "\t\t\t}\n" +
                            "\t\t},\n" +
                            "\t\t\"strategy\": {\n" +
                            "\t\t\t\"type\": \"Recreate\"\n" +
                            "\t\t},\n" +
                            "\t\t\"revisionHistoryLimit\": 10,\n" +
                            "\t\t\"progressDeadlineSeconds\": 600\n" +
                            "\t},\n" +
                            "\t\"status\": {\n" +
                            "\t\t\"observedGeneration\": 1,\n" +
                            "\t\t\"replicas\": 1,\n" +
                            "\t\t\"updatedReplicas\": 1,\n" +
                            "\t\t\"readyReplicas\": 1,\n" +
                            "\t\t\"availableReplicas\": 1,\n" +
                            "\t\t\"conditions\": [\n" +
                            "\t\t\t{\n" +
                            "\t\t\t\t\"type\": \"Progressing\",\n" +
                            "\t\t\t\t\"status\": \"True\",\n" +
                            "\t\t\t\t\"lastUpdateTime\": \"2023-07-06T11:43:45Z\",\n" +
                            "\t\t\t\t\"lastTransitionTime\": \"2023-07-06T11:41:42Z\",\n" +
                            "\t\t\t\t\"reason\": \"NewReplicaSetAvailable\",\n" +
                            "\t\t\t\t\"message\": \"ReplicaSet \\\"archivecleanup-worker-867b8bc68d\\\" has successfully progressed.\"\n" +
                            "\t\t\t},\n" +
                            "\t\t\t{\n" +
                            "\t\t\t\t\"type\": \"Available\",\n" +
                            "\t\t\t\t\"status\": \"True\",\n" +
                            "\t\t\t\t\"lastUpdateTime\": \"2023-07-07T05:37:41Z\",\n" +
                            "\t\t\t\t\"lastTransitionTime\": \"2023-07-07T05:37:41Z\",\n" +
                            "\t\t\t\t\"reason\": \"MinimumReplicasAvailable\",\n" +
                            "\t\t\t\t\"message\": \"Deployment has minimum availability.\"\n" +
                            "\t\t\t}\n" +
                            "\t\t]\n" +
                            "\t}\n" +
                            "}"));

            mockK8sServer.start();
//            final String mockK8sUrl = mockK8sServer.url("/").toString();
////
//            final Kubectl kubectl = mock(Kubectl.class);
//
//            when(kubectl.get(V1Deployment.class).namespace(kubernetesNamespace)).thenReturn(mockK8sUrl);

            TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = new TargetQueuePerformanceMetricsProvider(queuesApi);
            final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maxConsumptionRateHistorySize, minConsumptionRateHistorySize);
            final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(roundingMultiple);
            final TargetQueueSettings targetQueueSettings = new TargetQueueSettings(currentTargetQueueLength, eligibleForRefillPercent,
                    maxInstances, currentInstances);

            final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = new TunedTargetQueueLengthProvider(
                    targetQueuePerformanceMetricsProvider,
                    historicalConsumptionRate,
                    roundTargetQueueLength,
                    noOpMode,
                    queueProcessingTimeGoalSeconds);

            final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
            when(targetQueueSettingsProvider.get(argThat(new QueueNameMatcher(queueName)))).thenReturn(targetQueueSettings);

            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, tunedTargetQueueLengthProvider);
            final StagingTargetPairProvider stagingTargetPairProvider = new StagingTargetPairProvider();

            final LowLevelDistributor lowLevelDistributor = new LowLevelDistributor(
                    queuesApi,
                    connectionFactory,
                    consumptionTargetCalculator,
                    stagingTargetPairProvider,
                    10000,
                    600000);

            lowLevelDistributor.runOnce(connection);

        }
    }

    private static class QueueNameMatcher implements ArgumentMatcher<Queue> {

        private final String queueName;

        public QueueNameMatcher(final String queueName) {

            this.queueName = queueName;
        }

        @Override
        public boolean matches(final Queue queue) {
            return queueName.equals(queue.getName());
        }
    }
}

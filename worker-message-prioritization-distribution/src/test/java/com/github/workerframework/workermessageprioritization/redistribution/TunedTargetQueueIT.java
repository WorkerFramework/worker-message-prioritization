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
import com.github.workerframework.workermessageprioritization.targetqueue.K8sTargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueuePerformanceMetricsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;
import com.github.workerframework.workermessageprioritization.targetqueue.TunedTargetQueueLengthProvider;
import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.internal.MockWebServerExtension;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

@ExtendWith(MockWebServerExtension.class)
public class TunedTargetQueueIT extends DistributorTestBase {

    final String queueName = "elastic-query-worker";
    final String stagingQueue1Name = getStagingQueueName(queueName, T1_STAGING_QUEUE_NAME);
    final String stagingQueue2Name = getStagingQueueName(queueName, T2_STAGING_QUEUE_NAME);

    // This test is for development purposes only
    // This test is to observe the consumption rate altering the recommended target queue length.
    // Console outputs and debugger should be used to see these dynamic changes.
    @Test
    public void tunedTargetQueueNoOpModeTest(final MockWebServer mockK8sServer) throws TimeoutException, IOException {
        try (final Connection connection = connectionFactory.newConnection()) {

            final int queueSize = 500000;
            final int sleepTime = 100;
            final int roundingMultiple = 100;
            final int maxConsumptionRateHistorySize = 100;
            final int minConsumptionRateHistorySize = 100;
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

            String k8sJson = IOUtils.toString(this.getClass().getResourceAsStream("/k8s/deploymentList.json"),"UTF-8");
            String apiVersions = IOUtils.toString(this.getClass().getResourceAsStream("/k8s/apiVersions.json"),"UTF-8");
            String apiResourceList = IOUtils.toString(this.getClass().getResourceAsStream("/k8s/apiResourceList.json"),"UTF-8");
            String apiGroupList = IOUtils.toString(this.getClass().getResourceAsStream("/k8s/apiGroupList.json"),"UTF-8");

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(apiVersions));

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(apiResourceList));

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(apiGroupList));

            IntStream.range(1, 21).forEach(i -> mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(apiResourceList)));

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(k8sJson));

            mockK8sServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("content-type", "application/json")
                    .setBody(k8sJson));

            mockK8sServer.start();

            final String mockServerUrl = "http://localhost:" + mockK8sServer.getPort();

            String localKubeConfig = "apiVersion: v1\n" +
                    "clusters:\n" +
                    "- cluster:\n" +
                    "    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUMvakNDQWVhZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJeE1UQXlPREV3TlRjd05Gb1hEVE14TVRBeU5qRXdOVGN3TkZvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBUGJOCkZhekJ1blR2OUh0cU1ZQXB0SU42NXZiYmhHWDVoMVJsc08xd0hGVTdGd21rb2sybkx2WEZvUVcwdTh3OGt3TDIKKzFEV2tVVFhoQmtVbUlWK2lMbWJwOUNwSzFjcmM4U09XWWFpbVZLSHFlbWhmTm5POFJKM3BCOHVnVC9BT0hDaQpmbXo1RkI2UGV6djRLN24rcFY5bDh6RnJqdGhKUERGdDRZYTRKS09UUlFCcmZIZUhzd0pUMFR0bFRCaUUwSjFXCkpvaG5iVWpFZ2JuQUF0WWN1Q0h2aStSSVlER0JyNG44dWk0OC9BWndUVzhjMXVJekFNUjNVS200eENESXRvNmsKSzlxM2h5U2pOUkxwMW1OUS9PcDVPamMzV3JRNnI4bytCVkNuUjRuL2tvc1RLZklaVitqMkUzQXVKc1d3ZmNlWgpaa2dsYkgvV3NOR2dzTlZRTFpzQ0F3RUFBYU5aTUZjd0RnWURWUjBQQVFIL0JBUURBZ0trTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0hRWURWUjBPQkJZRUZML1VGcXQ0V1NlZ2tQdHQ4dk8vYzVOck1OZkZNQlVHQTFVZEVRUU8KTUF5Q0NtdDFZbVZ5Ym1WMFpYTXdEUVlKS29aSWh2Y05BUUVMQlFBRGdnRUJBQW5YRWVvN0dFNEZ4MnNhQ1N6TAp3eW55cnJBb2MrVXppRWQvcXFWNTcxTmJJUmlFZkc1YURBcStJU1BrUlFNZGcrd0x3Ylg2R0FDUnhVV1VlOUdhCmZzV2JnVVIvSnBSZDFUUTQ5MFN3OHhWR1dJeDU3RGxvL1lQOEJzTW4xUjdEd2pFcUNGZ3Rvd09XVkR2bldPR3cKbUdMcTAzTlhFeVZ5Z0pRelUzQk5RNk5PM2VLRHo1NWZKTTRMOXE2bDIxOExCTGo3VDgxSFZLdHF2NzdmWWpFMQpQOGNtRkwzbnlDNEhaRmNTUjF1NWs5TzZiei9LQWNuNDFBQ3RMMU1lV2hJcEdyc0xwTVhkUkEzMThaU2JOMGUvCmI5MXkzLzkzUjIrOUZSNjYzc1R1ZkNsQUNscHRuTDIySitXNFBmSHEwTTk3aHd4MjVPTDlQaFN3T2FGSHoyMG4KL2lrPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==\n" +
                    "    server: " + mockServerUrl + "\n" +
                    "  name: local-k8s\n" +
                    "contexts:\n" +
                    "- context:\n" +
                    "    cluster: local-k8s\n" +
                    "    user: fas-deploy\n" +
                    "  name: fas-deploy@local-k8s\n" +
                    "current-context: fas-deploy@local-k8s\n" +
                    "kind: Config\n" +
                    "preferences: {}\n" +
                    "users:\n" +
                    "- name: fas-deploy\n" +
                    "  user:\n" +
                    "    token: eyJhbGciOiJSUzI1NiIsImtpZCI6Ijdmak5rZGxzUXJxRndwUTBoUk5jSEV5b09lOE90MmpSQ0JNNmVxaE5iaEkifQ" +
                    ".eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJwcml2YXRlIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImZhcy1kZXBsb3ktdG9rZW4tYzdiZzciLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC5uYW1lIjoiZmFzLWRlcGxveSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50LnVpZCI6ImUxNDNkYjEwLTk2NGEtNDBjYS1iNWYwLWUzNjUzZDM2NTdmNCIsInN1YiI6InN5c3RlbTpzZXJ2aWNlYWNjb3VudDpwcml2YXRlOmZhcy1kZXBsb3kifQ.Jlq-uIy15qFT5VM7GMQVZUvxq_LCy1JUsFnV8sswvvdCtZZ5jwDsDrPR4JaIA-WBF8hOXwTrxI27fZ-VkzcD2Qpz2Trj6aPvqG_ZgzmBLOnWChHEFAbktHwvF6CyKNtvV2AUwY_ZV2tXjImc-aTmxp6tvceDVwBrwOcJ3wJeKSEEPVNWKiPFhMaovQO9dq302tGDNA3GkfH9NPEBDVPVZH1mrWXkARuI3Jd2XnhycU5qNJ_mdGfA3eLml2BInaFFCSnkHRbYlyiR-F5b7YUUvpG50tlnzx9_GEr1YrGZtza1A5eaNalKO5lsnlFh6Nr23luuMvCnnfts01hG1Ctz2Q";


            final ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new StringReader(localKubeConfig))).build();

            TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider = new TargetQueuePerformanceMetricsProvider(queuesApi);
            final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maxConsumptionRateHistorySize, minConsumptionRateHistorySize);
            final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(roundingMultiple);

            final TunedTargetQueueLengthProvider tunedTargetQueueLengthProvider = new TunedTargetQueueLengthProvider(
                    targetQueuePerformanceMetricsProvider,
                    historicalConsumptionRate,
                    roundTargetQueueLength,
                    noOpMode,
                    queueProcessingTimeGoalSeconds);

            List<String> namespaces = new ArrayList<>();
            namespaces.add("private");

            final K8sTargetQueueSettingsProvider k8sTargetQueueSettingsProvider = new K8sTargetQueueSettingsProvider(
                                        namespaces,
                    60,
                                        client);

            final ConsumptionTargetCalculator consumptionTargetCalculator =
                    new EqualConsumptionTargetCalculator(k8sTargetQueueSettingsProvider, tunedTargetQueueLengthProvider);
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
}

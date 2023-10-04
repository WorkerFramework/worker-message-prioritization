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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.redistribution.EnvVariableCollector;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.AbstractMap;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StagingQueueWeightSettingsTest {

    @Test
    public void getQueueWeightTest() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);
        final Queue q2 = getQueue("bulk-indexer-in»/rory3/enrichment-workflow", 1000);
        final Queue q3 = getQueue("dataprocessing-classification-in»/clynch/update-entities-workflow", 1000);
        final Queue q4 = getQueue("dataprocessing-classification-in»/a77777/update-entities-workflow", 1000);
        final Queue q5 = getQueue("dataprocessing-classification-in»/rory3/update-entities-workflow", 1000);
        final Queue q6 = getQueue("bulk-indexer-in»/clynch/a77777", 1000);
        final Queue q7 = getQueue("dataprocessing-langdetect-in»/mahesh/ingestion-workflow", 1000);
        final Queue q8 = getQueue("bulk-indexer-in»/jmcc02/repository-initialization-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6, q7, q8));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Set<Map.Entry<String, String>> envVariables = new HashSet<>();
        final Map.Entry<String, String> env1 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT", "enrichment\\-workflow$,10");
        final Map.Entry<String, String> env2 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_1", "clynch,0");
        final Map.Entry<String, String> env3 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_2", "a77777,3");
        final Map.Entry<String, String> env4 =
                new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_3", "dataprocessing\\-langdetect\\-in,7");
        final Map.Entry<String, String> env5 =
                new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_4", "repository\\-initialization\\-workflow$,0.5");
        final Map.Entry<String, String> env6 = new AbstractMap.SimpleEntry<>("FAST_LANE_LOG_LEVEL", "DEBUG");

        envVariables.add(env1);
        envVariables.add(env2);
        envVariables.add(env3);
        envVariables.add(env4);
        envVariables.add(env5);
        envVariables.add(env6);

        try(MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class)) {

            envVariableCollectorMock.when(EnvVariableCollector::getEnvVariables).thenReturn(envVariables);

            final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                    new StagingQueueWeightSettingsProvider();

            final List<String> stagingQueueNames =
                    distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

            final Map<String, Double> stagingQueueWeightMap =
                    stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames);

            assertEquals("Weight of queue should be set by environment variable.",
                    10, stagingQueueWeightMap.get("bulk-indexer-in»/clynch/enrichment-workflow"), 0.0);
            assertEquals("Weight of queue should be set by environment variable which matches the longest length of string.",
                    10, stagingQueueWeightMap.get("bulk-indexer-in»/rory3/enrichment-workflow"), 0.0);
            assertEquals("Weight of queue should be set by environment variable.",
                    0, stagingQueueWeightMap.get("dataprocessing-classification-in»/clynch/update-entities-workflow"), 0.0);
            assertEquals("Weight of queue should be set by environment variable.",
                    3, stagingQueueWeightMap.get("dataprocessing-classification-in»/a77777/update-entities-workflow"), 0.0);
            assertEquals("No weight set to match this string therefore should default to 1.",
                    1, stagingQueueWeightMap.get("dataprocessing-classification-in»/rory3/update-entities-workflow"), 0.0);
            assertEquals("Two strings matched with different weights should set to larger weight.",
                    3, stagingQueueWeightMap.get("bulk-indexer-in»/clynch/a77777"), 0.0);
            assertEquals("Two strings matched with different weights should set to larger weight.",
                    7, stagingQueueWeightMap.get("dataprocessing-langdetect-in»/mahesh/ingestion-workflow"), 0.0);
            assertEquals("Weights can be set below 0 to reduce the processing of the queue.",
                    0.5, stagingQueueWeightMap.get("bulk-indexer-in»/jmcc02/repository-initialization-workflow"), 0.0);
        }
    }

    Queue getQueue(final String name, final long messages) {
        final Queue queue = new Queue();
        queue.setName(name);
        queue.setMessages(messages);
        return queue;
    }
}

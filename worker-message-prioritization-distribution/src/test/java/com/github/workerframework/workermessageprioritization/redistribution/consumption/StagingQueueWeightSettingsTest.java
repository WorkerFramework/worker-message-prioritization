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
    public void calculateQueueWeight() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);
        final Queue q2 = getQueue("bulk-indexer-in»/rory3/enrichment-workflow", 1000);
        final Queue q3 = getQueue("dataprocessing-classification-in»/clynch/update-entities-workflow", 1000);
        final Queue q4 = getQueue("dataprocessing-classification-in»/a7777/update-entities-workflow", 1000);
        final Queue q5 = getQueue("dataprocessing-classification-in»/rory3/update-entities-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Set<Map.Entry<String, String>> envVariables = new HashSet<>();
        final Map.Entry<String, String> env1 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT", "enrichment\\-workflow$,10");
        final Map.Entry<String, String> env2 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_1", "clynch,0");
        final Map.Entry<String, String> env3 = new AbstractMap.SimpleEntry<>("CAF_ADJUST_WORKER_WEIGHT_1", "a7777,3");
        final Map.Entry<String, String> env4 = new AbstractMap.SimpleEntry<>("FAST_LANE_LOG_LEVEL", "DEBUG");

        envVariables.add(env1);
        envVariables.add(env2);
        envVariables.add(env3);
        envVariables.add(env4);

        MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class);

        envVariableCollectorMock.when(EnvVariableCollector::getEnvVariables).thenReturn(envVariables);

        final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                new StagingQueueWeightSettingsProvider();

        final List<String> stagingQueueNames =
                distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

        final Map<String, Double> stagingQueueWeightMap =
                stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames);

        assertEquals("Weight of queue should be set by environment variable.",
                10, stagingQueueWeightMap.get("bulk-indexer-in»/clynch/enrichment-workflow"), 0.0);
        assertEquals("Weight of queue should be set by environment variable.",
                10, stagingQueueWeightMap.get("bulk-indexer-in»/rory3/enrichment-workflow"), 0.0);
        assertEquals("Weight of queue should be set by environment variable.",
                0, stagingQueueWeightMap.get("dataprocessing-classification-in»/clynch/update-entities-workflow"), 0.0);
        assertEquals("Weight of queue should be set by environment variable.",
                3, stagingQueueWeightMap.get("dataprocessing-classification-in»/a7777/update-entities-workflow"), 0.0);
        assertEquals("Weight of queue should be set by environment variable.",
                1, stagingQueueWeightMap.get("dataprocessing-classification-in»/rory3/update-entities-workflow"), 0.0);
    }

    Queue getQueue(final String name, final long messages) {
        final Queue queue = new Queue();
        queue.setName(name);
        queue.setMessages(messages);
        return queue;
    }
}

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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.redistribution.EnvVariableCollector;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StagingQueueWeightSettingsTest {

    @Test
    public void getQueueWeightTest() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);
        final Queue q2 = getQueue("bulk-indexer-in»/rory3/enrichment-workflow", 1000);
        final Queue q3 = getQueue("dataprocessing-classification-in»/clynch/update-entities-workflow", 1000);
        final Queue q4 = getQueue("dataprocessing-classification-in»/rory3/update-entities-workflow", 1000);
        final Queue q5 = getQueue("bulk-indexer-in»/rtorney/maheshh", 1000);
        final Queue q6 = getQueue("bulk-indexer-in»/jmcc02/repository-initialization-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2, q3, q4, q5, q6));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Map<String, String> envVariables = new HashMap<>();

        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT", "/enrichment\\-workflow$,10");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_1", ".*/clynch/.*,0");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_2", "rtorney,5");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_3", "maheshh,6");
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_6", "repository\\-initialization\\-workflow$,0.5");

        try (final MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class)) {

            envVariableCollectorMock.when(() -> EnvVariableCollector.getQueueWeightEnvVariables(anyMap())).thenReturn(envVariables);

            final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                    new StagingQueueWeightSettingsProvider();

            final List<String> stagingQueueNames =
                    distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

            final Map<String, Double> stagingQueueWeightMap =
                    stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames);

            assertEquals((Double)0D, stagingQueueWeightMap.get("bulk-indexer-in»/clynch/enrichment-workflow"));
            assertEquals((Double)10D, stagingQueueWeightMap.get("bulk-indexer-in»/rory3/enrichment-workflow"),
                    "Weight of queue should be set by environment variable matching the longest length of string." +
                            "Which in this case is the workflow at the end of the string");
            assertEquals((Double)1D, stagingQueueWeightMap.get("dataprocessing-classification-in»/rory3/update-entities-workflow"),
                    "No weight set to match this string therefore should default to 1.");
            assertEquals((Double)6D, stagingQueueWeightMap.get("bulk-indexer-in»/rtorney/maheshh"),
                    "Two strings matched of same length with different weights should set to larger weight.");
            assertEquals((Double)0.5D, stagingQueueWeightMap.get("bulk-indexer-in»/jmcc02/repository-initialization-workflow"),
                    "Weight should be a decimal value below 1.");
        }
    }

    @Test
    public void passIncorrectEnvVariableFormatWithSpaceTest() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Collections.singletonList(q1));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Map<String, String> envVariables = new HashMap<>();

        // String cannot have space before weight
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT", "enrichment\\-workflow$, 10");

        try (final MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class)) {

            envVariableCollectorMock.when(() -> EnvVariableCollector.getQueueWeightEnvVariables(anyMap())).thenReturn(envVariables);

            final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                    new StagingQueueWeightSettingsProvider();

            final List<String> stagingQueueNames =
                    distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

            final IllegalArgumentException illegalArgumentException =  assertThrows(IllegalArgumentException.class, 
                    () -> stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames));
            
            assertEquals(
                    "Illegal format for CAF_ADJUST_QUEUE_WEIGHT string: 'enrichment\\-workflow$, 10'. " +
                            "Please ensure there are no spaces in the string, negative numbers or unnecessary zeros " +
                            "preceding the weight value.", 
                    illegalArgumentException.getMessage());
        }
    }

    @Test
    public void passIncorrectEnvVariableFormatWithZeroTest() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Collections.singletonList(q1));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Map<String, String> envVariables = new HashMap<>();

        // Weight cannot be preceeded by a 0
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT_1", "enrichment\\-workflow$,010");

        try (final MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class)) {

            envVariableCollectorMock.when(() -> EnvVariableCollector.getQueueWeightEnvVariables(anyMap())).thenReturn(envVariables);

            final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                    new StagingQueueWeightSettingsProvider();

            final List<String> stagingQueueNames =
                    distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

            final IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, 
                    () -> stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames));
            
            assertEquals(
                    "Illegal format for CAF_ADJUST_QUEUE_WEIGHT_1 string: 'enrichment\\-workflow$,010'. " +
                            "Please ensure there are no spaces in the string, negative numbers or unnecessary zeros" +
                            " preceding the weight value.", 
                    illegalArgumentException.getMessage());
        }
    }

    @Test
    public void passIncorrectEnvVariableFormatWithNegativeWeightTest() {

        final Queue targetQueue = getQueue("tq", 1000);

        final Queue q1 = getQueue("bulk-indexer-in»/clynch/enrichment-workflow", 1000);

        final Set<Queue> stagingQueues = new HashSet<>(Collections.singletonList(q1));
        final DistributorWorkItem distributorWorkItem = mock(DistributorWorkItem.class);
        when(distributorWorkItem.getStagingQueues()).thenReturn(stagingQueues);
        when(distributorWorkItem.getTargetQueue()).thenReturn(targetQueue);

        // Mock environment variables to set weights using regex pattern followed by weight.
        final Map<String, String> envVariables = new HashMap<>();

        // String cannot be negative
        envVariables.put("CAF_ADJUST_QUEUE_WEIGHT", "enrichment\\-workflow$,-10");

        try (final MockedStatic<EnvVariableCollector> envVariableCollectorMock = Mockito.mockStatic(EnvVariableCollector.class)) {

            envVariableCollectorMock.when(() -> EnvVariableCollector.getQueueWeightEnvVariables(anyMap())).thenReturn(envVariables);

            final StagingQueueWeightSettingsProvider stagingQueueWeightSettingsProvider =
                    new StagingQueueWeightSettingsProvider();

            final List<String> stagingQueueNames =
                    distributorWorkItem.getStagingQueues().stream().map(Queue::getName).collect(toList());

            final IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, 
                    () -> stagingQueueWeightSettingsProvider.getStagingQueueWeights(stagingQueueNames));
            
            assertEquals("Illegal format for CAF_ADJUST_QUEUE_WEIGHT string: 'enrichment\\-workflow$,-10'. " +
                            "Please ensure there are no spaces in the string, negative numbers or unnecessary zeros " +
                            "preceding the weight value.", 
                    illegalArgumentException.getMessage());
        }
    }

    private static Queue getQueue(final String name, final long messages) {
        final Queue queue = new Queue();
        queue.setName(name);
        queue.setMessages(messages);
        return queue;
    }
}

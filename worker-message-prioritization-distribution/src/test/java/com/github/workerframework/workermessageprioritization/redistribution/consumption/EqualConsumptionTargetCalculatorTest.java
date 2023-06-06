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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class EqualConsumptionTargetCalculatorTest
{

    @Test
    public void calculateConsumptionTargetsTest()
    {
        final TargetQueueSettingsProvider provider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 20);
        when(provider.get(targetQueue)).thenReturn(settings);

        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(50)));

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(provider);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(stagingQueues);

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertFalse(consumptionTargets.isEmpty());
        assertEquals(125, consumptionTargets.get(q1).longValue());
    }

    @Test
    public void calculateConsumptionTargetsTestReturn0()
    {
        final TargetQueueSettingsProvider provider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 20);
        when(provider.get(targetQueue)).thenReturn(settings);

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(provider);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(Collections.emptySet());

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertTrue(consumptionTargets.isEmpty());
    }
}

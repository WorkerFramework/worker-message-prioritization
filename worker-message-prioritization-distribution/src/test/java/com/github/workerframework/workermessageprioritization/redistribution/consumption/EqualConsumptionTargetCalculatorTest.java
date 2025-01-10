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
package com.github.workerframework.workermessageprioritization.redistribution.consumption;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorTestBase;
import com.github.workerframework.workermessageprioritization.redistribution.DistributorWorkItem;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class EqualConsumptionTargetCalculatorTest extends DistributorTestBase
{

    @Test
    public void calculateConsumptionTargetsTest()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(200)));

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCapacity()).thenReturn(250L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(stagingQueues);

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertFalse(consumptionTargets.isEmpty());
        assertEquals(125, consumptionTargets.get(q1).longValue());
    }

    @Test
    public void calculateConsumptionTargetsTestEmptyStagingQueueSetReturn0()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCapacity()).thenReturn(250L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(Collections.emptySet());

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertTrue(consumptionTargets.isEmpty());
    }

    @Test
    public void calculateConsumptionTargetTestInsufficientCapacityInTargetQueueReturn0()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(900);

        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(50)));

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCapacity()).thenReturn(0L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(stagingQueues);

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertFalse(consumptionTargets.isEmpty());
        assertEquals(0, consumptionTargets.get(q1).longValue());
    }

    @Test
    public void calculateConsumptionTargetsTestStagingQueueContainsLessMessagesThanTargetQueueHasCapacityFor()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);

        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final Queue q1 = new Queue();
        final Queue q2 = new Queue();
        final Set<Queue> stagingQueues = new HashSet<>(Arrays.asList(q1, q2));
        stagingQueues.forEach((queue -> queue.setMessages(50)));

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCapacity()).thenReturn(250L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final EqualConsumptionTargetCalculator equalCalculator = new EqualConsumptionTargetCalculator(targetQueueSettingsProvider, capacityCalculatorBase);

        final DistributorWorkItem workItem = mock(DistributorWorkItem.class);
        when(workItem.getTargetQueue()).thenReturn(targetQueue);
        when(workItem.getStagingQueues()).thenReturn(stagingQueues);

        final Map<Queue, Long> consumptionTargets = equalCalculator.calculateConsumptionTargets(workItem);

        assertFalse(consumptionTargets.isEmpty());
        assertEquals(50, consumptionTargets.get(q1).longValue());
    }
}

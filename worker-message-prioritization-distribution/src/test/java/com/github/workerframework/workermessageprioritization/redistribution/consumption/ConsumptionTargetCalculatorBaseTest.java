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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import com.github.workerframework.workermessageprioritization.targetqueue.CapacityCalculatorBase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public final class ConsumptionTargetCalculatorBaseTest
{

//    @Test
    public void getTargetQueueCapacityRegardlessOfRefillPercentageTest()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(1000);

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCurrentMaxLength()).thenReturn(1000L);
        when(targetQueueSettings.getEligibleForRefillPercentage()).thenReturn(20L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final ConsumptionTargetCalculatorBase calculator = mock(
            ConsumptionTargetCalculatorBase.class,
            withSettings().useConstructor(targetQueueSettingsProvider, capacityCalculatorBase).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals(200, calculator.getTargetQueueCapacity(targetQueue),
                "Message capacity available returned regardless of the eligible for refill percentage set.");
    }

//    @Test
    public void getTargetQueueCapacityTestMessageCountAboveLimitReturn0()
    {
        final TargetQueueSettingsProvider targetQueueSettingsProvider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(1200);

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCurrentMaxLength()).thenReturn(1000L);
        when(targetQueueSettings.getEligibleForRefillPercentage()).thenReturn(20L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final ConsumptionTargetCalculatorBase calculator = mock(
                ConsumptionTargetCalculatorBase.class,
                withSettings().useConstructor(targetQueueSettingsProvider, capacityCalculatorBase).defaultAnswer(CALLS_REAL_METHODS));


        assertEquals(0, calculator.getTargetQueueCapacity(targetQueue),
                "There are more messages on the queue than the set maximum target queue length available therefore no space " +
                        "available and 0 returned.");
    }
}

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
import com.github.workerframework.workermessageprioritization.targetqueue.*;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MinimumConsumptionTargetCalculatorTest
{

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 20%
     *
     * Therefore, messages should be moved - expect 250 to be returned
     */
    @Test
    public void getTargetQueueCapacityWithAdequateRefillPercentageTest()
    {
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCurrentMaxLength()).thenReturn(1000L);
        when(targetQueueSettings.getEligibleForRefillPercentage()).thenReturn(20L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final MinimumCapacityCalculator minimumCapacityCalculator = new MinimumCapacityCalculator(null);
        assertEquals("Adequate percentage of message space available therefore this capacity is returned.", 250,
                minimumCapacityCalculator.refine(targetQueue, targetQueueSettings).getCapacity());
    }

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 30%
     *
     * Therefore, no messages should be moved - expect 0 to be returned
     */
    @Test
    public void getTargetQueueCapacityWithoutAdequateRefillPercentageTestReturn0()
    {
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(1000);

        final TargetQueueSettings targetQueueSettings = mock(TargetQueueSettings.class);
        when(targetQueueSettings.getCurrentMaxLength()).thenReturn(1200L);
        when(targetQueueSettings.getEligibleForRefillPercentage()).thenReturn(25L);

        final CapacityCalculatorBase capacityCalculatorBase = mock(CapacityCalculatorBase.class);
        when(capacityCalculatorBase.refine(any(), any())).thenReturn(targetQueueSettings);

        final MinimumCapacityCalculator minimumCapacityCalculator = new MinimumCapacityCalculator(capacityCalculatorBase);

        assertEquals("The space available is less than the percentage of space required for refill, therefore 0 returned.", 0,
                minimumCapacityCalculator.refine(targetQueue, targetQueueSettings).getCapacity());
    }
}

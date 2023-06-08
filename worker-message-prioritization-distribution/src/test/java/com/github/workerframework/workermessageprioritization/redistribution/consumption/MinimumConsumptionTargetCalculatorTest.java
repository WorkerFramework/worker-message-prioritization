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
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettingsProvider;
import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueSettings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.when;

public final class MinimumConsumptionTargetCalculatorTest
{

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 20%
     *
     * Therefore, messages should be moved - expect 250 to be returned
     */
    @Test
    public void getTargetQueueCapacityTest()
    {
        final TargetQueueSettingsProvider provider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 20);
        when(provider.get(targetQueue)).thenReturn(settings);

        final MinimumConsumptionTargetCalculator minimumConsumptionTargetCalculator = mock(
            MinimumConsumptionTargetCalculator.class,
            withSettings().useConstructor(provider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals(250, minimumConsumptionTargetCalculator.getTargetQueueCapacity(targetQueue));
    }

    /**
     * Target Queue Max Length: 1000 Target Queue Available Capacity: 750 (25%) Target Queue Eligible For Refill Percentage: 30%
     *
     * Therefore, no messages should be moved - expect 0 to be returned
     */
    @Test
    public void getTargetQueueCapacityTestReturn0()
    {
        final TargetQueueSettingsProvider provider = mock(TargetQueueSettingsProvider.class);
        final Queue targetQueue = new Queue();
        targetQueue.setMessages(750);

        final TargetQueueSettings settings = new TargetQueueSettings(1000, 30);
        when(provider.get(targetQueue)).thenReturn(settings);

        final MinimumConsumptionTargetCalculator minimumConsumptionTargetCalculator = mock(
            MinimumConsumptionTargetCalculator.class,
            withSettings().useConstructor(provider).defaultAnswer(CALLS_REAL_METHODS));

        assertEquals(0, minimumConsumptionTargetCalculator.getTargetQueueCapacity(targetQueue));
    }
}
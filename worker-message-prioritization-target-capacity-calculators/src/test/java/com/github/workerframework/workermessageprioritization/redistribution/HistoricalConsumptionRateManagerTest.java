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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRateManager;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HistoricalConsumptionRateManagerTest {
    private final int maximumConsumptionRateHistorySize = 100;
    private final int minConsumptionRateHistorySize = 10;
    private final String targetQueue1 = "targetQueue1";
    private final String targetQueue2 = "targetQueue2";

    @Test
    public void getErrorWhenMinConsumptionRateHistorySizeLargerThanMaxConsumptionRateHistorySizeTest() {

        assertThrows(IllegalArgumentException.class, () -> {
            final HistoricalConsumptionRateManager historicalConsumptionRateManager =
                    new HistoricalConsumptionRateManager(10,
                    100);
        });
    }

    @Test
    public void getErrorWhenQueueDoesNotExistTest() {

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        assertThrows(IllegalArgumentException.class, () -> {
            historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue1);
        });
    }

    @Test
    public void isSufficientHistoryAvailableForMultipleQueuesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final double messageBytesReady = 1D;

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IntStream.range(0, minConsumptionRateHistorySize).forEach(i ->
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, 
                    theoreticalConsumptionRate1, messageBytesReady));

        IntStream.range(0, minConsumptionRateHistorySize + 5).forEach(i ->
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, 
                    theoreticalConsumptionRate2, messageBytesReady));

        final boolean isSufficientHistoryAvailableQueue1 = 
                historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue1);

        final boolean isSufficientHistoryAvailableQueue2 = 
                historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue2);

        assertTrue("Should return true as this is set to provide the minimum consumption rate history.",
                isSufficientHistoryAvailableQueue1);
        assertTrue("Should return true as this is set to provide above the minimum consumption rate history.",
                isSufficientHistoryAvailableQueue2);
    }

    @Test
    public void isSufficientHistoryAvailableWithMinConsumptionRateHistorySetToZeroTest(){

        final int noMinConsumptionRateHistorySizeRequired = 0;

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                noMinConsumptionRateHistorySizeRequired);

        final boolean noMinimumRequiredHistorySet = 
                historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueue1);

        assertTrue("Should return true as minimum required consumption rate history has been set to zero.", 
                noMinimumRequiredHistorySet);
    }

    @Test
    public void consumptionRateAverageForEqualConsumptionRatesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final double messageBytesReady = 1;

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IntStream.range(0, minConsumptionRateHistorySize).forEach(i ->
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, 
                    theoreticalConsumptionRate1, messageBytesReady));

        IntStream.range(0, minConsumptionRateHistorySize + 5).forEach(i ->
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, 
                    theoreticalConsumptionRate2, messageBytesReady));

        final double theoreticalConsumptionRateHistoryQueue1Average =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                        theoreticalConsumptionRate1, messageBytesReady);
        final double theoreticalConsumptionRateHistoryQueue2Average =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                        theoreticalConsumptionRate2, messageBytesReady);

        assertEquals("This should return the average consumption rate for target queue 1.", 2.5,
                theoreticalConsumptionRateHistoryQueue1Average, 0.001);
        assertEquals("This should return the average consumption rate for target queue 2", 5.0,
                theoreticalConsumptionRateHistoryQueue2Average, 0.001);
    }

    @Test
    public void consumptionRateAverageForChangingConsumptionRatesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final double messageBytesReady = 1D;

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        final List<Double> targetQueue1ConsumptionRates = 
                Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        for(final Double rate: targetQueue1ConsumptionRates){
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, rate, 
                    messageBytesReady);
        }

        final List<Double> targetQueue2ConsumptionRates = 
                Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0);
        for(final Double rate: targetQueue2ConsumptionRates){
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, rate, 
                    messageBytesReady);
        }

        final double theoreticalConsumptionRateHistoryQueue1Average =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                        theoreticalConsumptionRate1, messageBytesReady);
        final double theoreticalConsumptionRateHistoryQueue2Average =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2,
                        theoreticalConsumptionRate2, messageBytesReady);

        assertEquals("This should return the average consumption rate for target queue 1", 4.791,
                theoreticalConsumptionRateHistoryQueue1Average, 0.001);
        assertEquals("This should return the average consumption rate for target queue 2", 7.352,
                theoreticalConsumptionRateHistoryQueue2Average, 0.001);
    }

    @Test
    public void consumptionRateNotRecordedIfNoMessageBytesReadyTest(){

        final double theoreticalConsumptionRate1 = 2.5;

        final double messageBytesReady = 0D;

        final HistoricalConsumptionRateManager historicalConsumptionRateManager = 
                new HistoricalConsumptionRateManager(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        final List<Double> targetQueue1ConsumptionRates = 
                Arrays.asList(2.5, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        for(final Double rate: targetQueue1ConsumptionRates){
            historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, rate, 
                    messageBytesReady);
        }

        final double theoreticalConsumptionRateHistoryQueue1Average =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1,
                        theoreticalConsumptionRate1, messageBytesReady);

        assertEquals(
                "For a queue with no message bytes ready, only the first consumption rate should be recorded " +
                        "to ensure the queue exists and has a history. The rest of the consumption rates should be " +
                        "discarded. In this case the average consumption rate should be equal to the first " +
                        "consumption rate passed.", 2.5,
                theoreticalConsumptionRateHistoryQueue1Average, 0.001);
    }
}

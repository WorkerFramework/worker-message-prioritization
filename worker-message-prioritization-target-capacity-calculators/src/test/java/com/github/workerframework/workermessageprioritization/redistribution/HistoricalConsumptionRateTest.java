package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.targetqueue.HistoricalConsumptionRate;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HistoricalConsumptionRateTest {
    private final int maximumConsumptionRateHistorySize = 100;
    private final int minConsumptionRateHistorySize = 10;
    private final String targetQueue1 = "targetQueue1";
    private final String targetQueue2 = "targetQueue2";

    @Test
    public void getErrorWhenMinConsumptionRateHistorySizeLargerThanMaxConsumptionRateHistorySizeTest() {

        IllegalArgumentException illegalArgumentException = null;
        try {
            final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(10,
                    100);
        } catch (final IllegalArgumentException exception) {
            illegalArgumentException = exception;
        }

        assertNotNull("IllegalArgumentException was not thrown", illegalArgumentException);
        assertEquals("Minimum history required cannot be larger than the maximum history storage size.",
                illegalArgumentException.getMessage());
    }

    @Test
    public void getErrorWhenQueueDoesNotExistTest() {

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IllegalArgumentException illegalArgumentException = null;
        try {
            historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1);
        } catch (final IllegalArgumentException exception) {
            illegalArgumentException = exception;
        }

        assertNotNull("IllegalArgumentException was not thrown", illegalArgumentException);
        assertEquals("Queue with this name not found.", illegalArgumentException.getMessage());
    }

    @Test
    public void consumptionRateHistorySizeTest(){

        final double theoreticalConsumptionRate = 2.5;

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IntStream.range(0, minConsumptionRateHistorySize).forEach(i ->
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate));

        final boolean theoreticalConsumptionRateHistory = historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1);

        assertTrue("Should return true as this is set to provide the minimum consumption rate history.",
                theoreticalConsumptionRateHistory);
    }

    @Test
    public void consumptionRateHistorySizeMultipleQueuesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IntStream.range(0, minConsumptionRateHistorySize).forEach(i ->
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate1));

        IntStream.range(0, minConsumptionRateHistorySize + 5).forEach(i ->
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, theoreticalConsumptionRate2));

        final boolean theoreticalConsumptionRateHistoryQueue1 = historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1);

        final boolean theoreticalConsumptionRateHistoryQueue2 = historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue2);

        assertTrue("Should return true as this is set to provide the minimum consumption rate history.",
                theoreticalConsumptionRateHistoryQueue1);
        assertTrue("Should return true as this is set to provide above the minimum consumption rate history.",
                theoreticalConsumptionRateHistoryQueue2);
    }

    @Test
    public void isSufficientHistoryAvailableWithMinConsumptionRateHistorySetToZero(){

        final double theoreticalConsumptionRate1 = 2.5;
        final int noMinConsumptionRateHistorySizeRequired = 0;

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                noMinConsumptionRateHistorySizeRequired);

        historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate1);

        final boolean noMinimumRequiredHistorySet = historicalConsumptionRate.isSufficientHistoryAvailable(targetQueue1);

        assertTrue("Should return true as minimum required consumption rate history has been set to zero.", noMinimumRequiredHistorySet);
    }

    @Test
    public void consumptionRateAverageForEqualConsumptionRatesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        IntStream.range(0, minConsumptionRateHistorySize).forEach(i ->
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate1));

        IntStream.range(0, minConsumptionRateHistorySize + 5).forEach(i ->
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, theoreticalConsumptionRate2));

        final double theoreticalConsumptionRateHistoryQueue1Average =
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate1);
        final double theoreticalConsumptionRateHistoryQueue2Average =
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, theoreticalConsumptionRate2);

        assertEquals("Should return true as this is set to provide the minimum consumption rate history.", 2.5,
                theoreticalConsumptionRateHistoryQueue1Average, 0.001);
        assertEquals("Should return true as this is set to provide above the minimum consumption rate history.", 5.0,
                theoreticalConsumptionRateHistoryQueue2Average, 0.001);
    }

    @Test
    public void consumptionRateAverageForChangingConsumptionRatesTest(){

        final double theoreticalConsumptionRate1 = 2.5;
        final double theoreticalConsumptionRate2 = 5.0;

        final HistoricalConsumptionRate historicalConsumptionRate = new HistoricalConsumptionRate(maximumConsumptionRateHistorySize,
                minConsumptionRateHistorySize);

        final List<Double> targetQueue1ConsumptionRates = Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);
        for(final Double rate: targetQueue1ConsumptionRates){
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, rate);
        }

        final List<Double> targetQueue2ConsumptionRates = Arrays.asList(0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0,
                12.0, 13.0, 14.0, 15.0);
        for(final Double rate: targetQueue2ConsumptionRates){
            historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, rate);
        }

        final double theoreticalConsumptionRateHistoryQueue1Average =
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue1, theoreticalConsumptionRate1);
        final double theoreticalConsumptionRateHistoryQueue2Average =
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueue2, theoreticalConsumptionRate2);

        assertEquals("This should return the average consumption rate for target queue 1", 4.791,
                theoreticalConsumptionRateHistoryQueue1Average, 0.001);
        assertEquals("This should return the average consumption rate for target queue 2", 7.352,
                theoreticalConsumptionRateHistoryQueue2Average, 0.001);
    }
}

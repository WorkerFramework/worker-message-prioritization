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
package com.github.workerframework.workermessageprioritization.targetqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TunedTargetQueueLengthProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(TunedTargetQueueLengthProvider.class);
    private final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider;
    private final boolean noOpMode;
    private final double queueProcessingTimeGoalSeconds;
    private final HistoricalConsumptionRate historicalConsumptionRate;
    private final RoundTargetQueueLength roundTargetQueueLength;

    public TunedTargetQueueLengthProvider (final TargetQueuePerformanceMetricsProvider targetQueuePerformanceMetricsProvider,
                                           final HistoricalConsumptionRate historicalConsumptionRate,
                                           final RoundTargetQueueLength roundTargetQueueLength,
                                           final boolean noOpMode,
                                           final double queueProcessingTimeGoalSeconds) {
        this.targetQueuePerformanceMetricsProvider = targetQueuePerformanceMetricsProvider;
        this.historicalConsumptionRate = historicalConsumptionRate;
        this.roundTargetQueueLength = roundTargetQueueLength;
        this.noOpMode = noOpMode;
        this.queueProcessingTimeGoalSeconds = queueProcessingTimeGoalSeconds;
    }

    public final long getTunedTargetQueueLength(final String targetQueueName, final long minTargetQueueLength,
                                                final long maxTargetQueueLength){

        final PerformanceMetrics performanceMetrics = targetQueuePerformanceMetricsProvider.getTargetQueuePerformanceMetrics(targetQueueName);

        final long metricsTargetQueueLength = performanceMetrics.getTargetQueueLength();
        final double metricsConsumptionRate = performanceMetrics.getConsumptionRate();
        final double metricsCurrentInstances = performanceMetrics.getCurrentInstances();
        final double metricsMaxInstances = performanceMetrics.getMaxInstances();

        logNoOpModeInformation();
        logCurrentTargetQueueInformation(metricsTargetQueueLength, targetQueueName);

        final double theoreticalConsumptionRate = calculateCurrentTheoreticalConsumptionRate(metricsConsumptionRate, metricsCurrentInstances,
                metricsMaxInstances);

        final double averageHistoricalConsumptionRate =
                historicalConsumptionRate.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueueName, theoreticalConsumptionRate);

        final long tunedTargetQueue = calculateTunedTargetQueue(averageHistoricalConsumptionRate);

        LOGGER.info("Considering the average theoretical consumption rate of this worker, the target queue length should be: " +
                tunedTargetQueue);

        final long roundedTargetQueue = roundAndCheckTargetQueue(tunedTargetQueue, maxTargetQueueLength, minTargetQueueLength);

        return determineFinalTargetQueueLength(targetQueueName, metricsTargetQueueLength, roundedTargetQueue);
    }

    private double calculateCurrentTheoreticalConsumptionRate(final double currentConsumptionRate, final double currentInstances,
                                                              final double maxInstances){
        return (currentConsumptionRate / currentInstances) * maxInstances;
    }

    private long calculateTunedTargetQueue(final double averageTheoreticalConsumptionRate){
        final double suggestedTargetQueueLength = averageTheoreticalConsumptionRate * queueProcessingTimeGoalSeconds;
        return (long) suggestedTargetQueueLength;
    }

    private long roundAndCheckTargetQueue(final long tunedTargetQueue, final long maxTargetQueueLength, final long minTargetQueueLength) {
        final long roundedTargetQueue = roundTargetQueueLength.getRoundedTargetQueueLength(tunedTargetQueue);
        LOGGER.info("In the case that the target queue length is rounded below the minimum target queue length or above the maximum target queue length. " +
                "Target queue length will be set to that minimum or maximum value respectively.");

        if (roundedTargetQueue > maxTargetQueueLength) {
            LOGGER.info("Rounded queue length: " + roundedTargetQueue + " exceeds the maximum length that the queue can be set to. " +
                    "Therefore the maximum length: "
                    + maxTargetQueueLength + " should be set.");
            return maxTargetQueueLength;
        }

        if (roundedTargetQueue < minTargetQueueLength) {
            LOGGER.info("Rounded queue length: " + roundedTargetQueue + " is less than the minimum length that the queue can be set to. Therefore the minimum length: "
                    + minTargetQueueLength + " should be set.");
            return minTargetQueueLength;
        } else {
            return roundedTargetQueue;
        }
    }

    private void logNoOpModeInformation(){
        if (noOpMode){
            LOGGER.info("noOpMode is on. The queue length will not be altered. Recommendations of how the queue length could " +
                    "be altered based on consumption rate will be logged.");
        }else{
            LOGGER.info("noOpMode is off. Provided there is sufficient consumption rate history of the worker, the target queue length " +
                    "will be altered.");
        }
    }

    private void logCurrentTargetQueueInformation(final long targetQueueLength, final String targetQueueName){
        LOGGER.info("Current target queue length for " + targetQueueName + ": " + targetQueueLength);
    }

    private long determineFinalTargetQueueLength(final String targetQueueName, final long targetQueueLength, final long tunedTargetQueue){

        if(noOpMode) {
            LOGGER.info("NoOpMode True - Target queue length has not been adjusted.");
            return targetQueueLength;
        }

        if(!historicalConsumptionRate.isSufficientHistoryAvailable(targetQueueName)) {
            LOGGER.info("Not enough ConsumptionRateHistory to make an adjustment.");
            return targetQueueLength;
        }

        if(targetQueueLength == tunedTargetQueue){
            LOGGER.info("Target queue is already set to optimum length: " + targetQueueLength + ". No action required.");
            return targetQueueLength;
        }else{
            LOGGER.info("Target queue length has been adjusted to: " + tunedTargetQueue);
            return tunedTargetQueue;
        }
    }
}

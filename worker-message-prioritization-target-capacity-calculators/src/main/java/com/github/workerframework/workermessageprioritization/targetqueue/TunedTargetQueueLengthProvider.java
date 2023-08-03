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
    private final QueueConsumptionRateProvider queueConsumptionRateProvider;
    private final boolean noOpMode;
    private final double queueProcessingTimeGoalSeconds;
    private final HistoricalConsumptionRateManager historicalConsumptionRateManager;
    private final TargetQueueLengthRounder targetQueueLengthRounder;

    public TunedTargetQueueLengthProvider (final QueueConsumptionRateProvider queueConsumptionRateProvider,
                                           final HistoricalConsumptionRateManager historicalConsumptionRateManager,
                                           final TargetQueueLengthRounder targetQueueLengthRounder,
                                           final boolean noOpMode,
                                           final double queueProcessingTimeGoalSeconds) {
        this.queueConsumptionRateProvider = queueConsumptionRateProvider;
        this.historicalConsumptionRateManager = historicalConsumptionRateManager;
        this.targetQueueLengthRounder = targetQueueLengthRounder;
        this.noOpMode = noOpMode;
        this.queueProcessingTimeGoalSeconds = queueProcessingTimeGoalSeconds;
    }

    public final long getTunedTargetQueueLength(final String targetQueueName, final long minTargetQueueLength,
                                                final long maxTargetQueueLength, final TargetQueueSettings targetQueueSettings){

        final double consumptionRate =
                queueConsumptionRateProvider.getConsumptionRate(targetQueueName);

        final double theoreticalConsumptionRate = calculateCurrentTheoreticalConsumptionRate(consumptionRate,
                targetQueueSettings.getCurrentInstances(), targetQueueSettings.getMaxInstances());

        final double averageHistoricalConsumptionRate =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueueName, theoreticalConsumptionRate);

        final long tunedTargetQueue = calculateTunedTargetQueue(averageHistoricalConsumptionRate);

        final long roundedTargetQueueLength = roundAndCheckTargetQueue(tunedTargetQueue, maxTargetQueueLength, minTargetQueueLength);

        return determineFinalTargetQueueLength(targetQueueName, targetQueueSettings.getCurrentMaxLength(), roundedTargetQueueLength);
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
        final long roundedTargetQueueLength = targetQueueLengthRounder.getRoundedTargetQueueLength(tunedTargetQueue);
        LOGGER.debug("In the case that the target queue length is rounded below the minimum target queue length or above the maximum " +
                "target queue length. Target queue length will be set to that minimum or maximum value respectively.");

        if (roundedTargetQueueLength > maxTargetQueueLength) {
            LOGGER.debug("Rounded queue length: {} exceeds the maximum length that the queue can be set to. " +
                    "Therefore the maximum length: {} should be set.", roundedTargetQueueLength, maxTargetQueueLength);
            return maxTargetQueueLength;
        }

        if (roundedTargetQueueLength < minTargetQueueLength) {
            LOGGER.debug("Rounded queue length: {} is less than the minimum length that the queue can be set to. Therefore the minimum " +
                    "length: {} should be set.", roundedTargetQueueLength, minTargetQueueLength);
            return minTargetQueueLength;
        } else {
            return roundedTargetQueueLength;
        }
    }

    private long determineFinalTargetQueueLength(final String targetQueueName, final long targetQueueLength, final long tunedTargetQueueLength){

        LOGGER.info("Current target queue length for {}: {}", targetQueueName, targetQueueLength);

        LOGGER.info("Recommended tuned target queue length is: {}", tunedTargetQueueLength);

        if(noOpMode) {
            LOGGER.info("NoOpMode True - Target queue length has not been adjusted.");
            return targetQueueLength;
        }

        if(!historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueueName)) {
            LOGGER.info("Not enough ConsumptionRateHistory to make an adjustment.");
            return targetQueueLength;
        }

        if(targetQueueLength == tunedTargetQueueLength){
            LOGGER.info("Target queue is already set to optimum length: {}. No action required.", targetQueueLength);
            return targetQueueLength;
        }else{
            LOGGER.info("Target queue length has been adjusted to: {}",tunedTargetQueueLength);
            return tunedTargetQueueLength;
        }
    }
}

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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TunedTargetQueueLengthProvider {

    private static final Logger TUNED_TARGET_LOGGER = LoggerFactory.getLogger("TUNED_TARGET");
    private final QueueConsumptionRateProvider queueConsumptionRateProvider;
    private final boolean noOpMode;
    private final double queueProcessingTimeGoalSeconds;
    private final HistoricalConsumptionRateManager historicalConsumptionRateManager;
    private final TargetQueueLengthRounder targetQueueLengthRounder;
    private final long minTargetQueueLength;
    private final long maxTargetQueueLength;

    @Inject
    public TunedTargetQueueLengthProvider (final QueueConsumptionRateProvider queueConsumptionRateProvider,
                                           final HistoricalConsumptionRateManager historicalConsumptionRateManager,
                                           final TargetQueueLengthRounder targetQueueLengthRounder,
                                           @Named("MinTargetQueueLength") final long minTargetQueueLength,
                                           @Named("MaxTargetQueueLength") final long maxTargetQueueLength,
                                           @Named("NoOpMode") final boolean noOpMode,
                                           @Named("QueueProcessingTimeGoalSeconds") final double queueProcessingTimeGoalSeconds) {
        this.queueConsumptionRateProvider = queueConsumptionRateProvider;
        this.historicalConsumptionRateManager = historicalConsumptionRateManager;
        this.targetQueueLengthRounder = targetQueueLengthRounder;
        this.minTargetQueueLength = minTargetQueueLength;
        this.maxTargetQueueLength = maxTargetQueueLength;
        this.noOpMode = noOpMode;
        this.queueProcessingTimeGoalSeconds = queueProcessingTimeGoalSeconds;
    }

    public final long getTunedTargetQueueLength(final String targetQueueName, final TargetQueueSettings targetQueueSettings){

        final double consumptionRate =
                queueConsumptionRateProvider.getConsumptionRate(targetQueueName);

        TUNED_TARGET_LOGGER.info("Current consumption rate of " + targetQueueName + "is: " + consumptionRate);

        final double theoreticalConsumptionRate = calculateCurrentTheoreticalConsumptionRate(consumptionRate,
                targetQueueSettings.getCurrentInstances(), targetQueueSettings.getMaxInstances());

        final double averageHistoricalConsumptionRate =
                historicalConsumptionRateManager.recordCurrentConsumptionRateHistoryAndGetAverage(targetQueueName, theoreticalConsumptionRate);

        final long tunedTargetQueue = calculateTunedTargetQueue(averageHistoricalConsumptionRate);

        final long roundedTargetQueueLength = roundAndCheckTargetQueue(tunedTargetQueue);

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

    private long roundAndCheckTargetQueue(final long tunedTargetQueue) {
        final long roundedTargetQueueLength = targetQueueLengthRounder.getRoundedTargetQueueLength(tunedTargetQueue);
        TUNED_TARGET_LOGGER.info("In the case that the target queue length is rounded below the minimum target queue length or above " +
                "the maximum " +
                "target queue length. Target queue length will be set to that minimum or maximum value respectively.");

        if (roundedTargetQueueLength > maxTargetQueueLength) {
            TUNED_TARGET_LOGGER.info("Rounded queue length: {} exceeds the maximum length that the queue can be set to. " +
                    "Therefore the maximum length: {} should be set.", roundedTargetQueueLength, maxTargetQueueLength);
            return maxTargetQueueLength;
        }

        if (roundedTargetQueueLength < minTargetQueueLength) {
            TUNED_TARGET_LOGGER.info("Rounded queue length: {} is less than the minimum length that the queue can be set to. Therefore" +
                    " the minimum " +
                    "length: {} should be set.", roundedTargetQueueLength, minTargetQueueLength);
            return minTargetQueueLength;
        } else {
            return roundedTargetQueueLength;
        }
    }

    private long determineFinalTargetQueueLength(final String targetQueueName, final long targetQueueLength, final long tunedTargetQueueLength){

        TUNED_TARGET_LOGGER.info("Current target queue length for {}: {}", targetQueueName, targetQueueLength);

        TUNED_TARGET_LOGGER.info("Recommended tuned target queue length is: {}", tunedTargetQueueLength);

        if(noOpMode) {
            TUNED_TARGET_LOGGER.info("NoOpMode True - Target queue length has not been adjusted.");
            return targetQueueLength;
        }

        if(!historicalConsumptionRateManager.isSufficientHistoryAvailable(targetQueueName)) {
            TUNED_TARGET_LOGGER.info("Not enough ConsumptionRateHistory to make an adjustment.");
            return targetQueueLength;
        }

        if(targetQueueLength == tunedTargetQueueLength){
            TUNED_TARGET_LOGGER.info("Target queue is already set to optimum length: {}. No action required.", targetQueueLength);
            return targetQueueLength;
        }else{
            TUNED_TARGET_LOGGER.info("Target queue length has been adjusted to: {}",tunedTargetQueueLength);
            return tunedTargetQueueLength;
        }
    }
}

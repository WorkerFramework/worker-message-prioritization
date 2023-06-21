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

import com.google.common.collect.EvictingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class HistoricalConsumptionRate {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistoricalConsumptionRate.class);
    private final int maximumConsumptionRateHistorySize;
    private final int minimumHistorySize;
    private final HashMap<String, EvictingQueue<Double>> consumptionRateHistoryMap = new HashMap<>();

    public HistoricalConsumptionRate(final int maximumConsumptionRateHistorySize, final int minimumHistorySize) throws IllegalArgumentException {

        if (maximumConsumptionRateHistorySize < minimumHistorySize) {
            LOGGER.error("Minimum history required cannot be larger than the maximum history storage size.");
            throw new IllegalArgumentException("Minimum history required cannot be larger than the maximum history storage size.");
        }

        this.maximumConsumptionRateHistorySize = maximumConsumptionRateHistorySize;
        this.minimumHistorySize = minimumHistorySize;
    }

    public double recordCurrentConsumptionRateHistoryAndGetAverage(final String targetQueueName, final double theoreticalConsumptionRate) {

        final EvictingQueue<Double> theoreticalConsumptionRateHistory;
        if (consumptionRateHistoryMap.containsKey(targetQueueName)) {
            theoreticalConsumptionRateHistory = consumptionRateHistoryMap.get(targetQueueName);
        } else {
            theoreticalConsumptionRateHistory = EvictingQueue.create(maximumConsumptionRateHistorySize);
            consumptionRateHistoryMap.put(targetQueueName, theoreticalConsumptionRateHistory);
        }
        theoreticalConsumptionRateHistory.add(theoreticalConsumptionRate);

        final double averageTheoreticalConsumptionRate = theoreticalConsumptionRateHistory.stream().mapToDouble(d -> d).average().getAsDouble();

        return averageTheoreticalConsumptionRate;
    }

    public boolean isSufficientHistoryAvailable(final String queueName) throws IllegalArgumentException {
        if (!consumptionRateHistoryMap.containsKey(queueName)) {
            throw new IllegalArgumentException("Queue with this name not found.");
        } else {
            final boolean isSufficientHistory = consumptionRateHistoryMap.get(queueName).size() >= minimumHistorySize;

            if (isSufficientHistory) {
                LOGGER.info("Consumption rate history from the last " + consumptionRateHistoryMap.get(queueName).size() +
                        " runs of this worker available. An average of these rates will determine the new target queue length. If " +
                        "different to the current queue length, the following suggestions will be implemented and the target queue " +
                        "length adjusted.");
            } else {
                LOGGER.info("There is not enough history to tune the target queue length accurately. The following logs are " +
                        "recommendations. The target queue will not be adjusted until more history is present.");
            }
            return isSufficientHistory;
        }
    }
}

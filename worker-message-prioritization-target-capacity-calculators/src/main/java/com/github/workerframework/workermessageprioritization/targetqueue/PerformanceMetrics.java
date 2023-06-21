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

public class PerformanceMetrics {
    private long targetQueueLength;
    private final double consumptionRate;
    private final double currentInstances;
    private final double maxInstances;

    public PerformanceMetrics(final long targetQueueLength, double consumptionRate, double currentInstances, double maxInstances) {
        this.targetQueueLength = targetQueueLength;
        this.consumptionRate = consumptionRate;
        this.currentInstances = currentInstances;
        this.maxInstances = maxInstances;
    }

    public long getTargetQueueLength() {
        return targetQueueLength;
    }

    public void setTargetQueueLength(final long tunedTargetQueueLength){
        this.targetQueueLength = tunedTargetQueueLength;
    }

    public double getConsumptionRate() {
        return consumptionRate;
    }

    public double getCurrentInstances() {
        return currentInstances;
    }

    public double getMaxInstances() {
        return maxInstances;
    }
}

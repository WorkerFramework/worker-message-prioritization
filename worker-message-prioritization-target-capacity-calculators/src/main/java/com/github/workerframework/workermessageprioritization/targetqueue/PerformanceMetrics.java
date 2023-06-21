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

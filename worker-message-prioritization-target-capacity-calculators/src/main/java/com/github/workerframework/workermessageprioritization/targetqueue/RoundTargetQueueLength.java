package com.github.workerframework.workermessageprioritization.targetqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundTargetQueueLength {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoundTargetQueueLength.class);

    private final int roundingMultiple;

    public RoundTargetQueueLength(final int roundingMultiple){
        this.roundingMultiple = roundingMultiple;
    }

    public final long getRoundedTargetQueueLength(final long tunedTargetQueue){
        final long remainder = tunedTargetQueue % roundingMultiple;
        final int roundingLimit = roundingMultiple / 2;
        final long roundedQueueLength;
        if(remainder == 0){
            return tunedTargetQueue;
        }
        if (remainder >= roundingLimit) {
            roundedQueueLength =  tunedTargetQueue - remainder + roundingMultiple;
        } else {
            roundedQueueLength =  tunedTargetQueue - remainder;
        }
        LOGGER.info("Target queue length has been rounded from: " + tunedTargetQueue + " to:" + roundedQueueLength);
        return roundedQueueLength;
    }
}

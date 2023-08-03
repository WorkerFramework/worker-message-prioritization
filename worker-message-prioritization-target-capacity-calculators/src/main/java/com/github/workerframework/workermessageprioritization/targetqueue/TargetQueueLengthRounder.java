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

public class TargetQueueLengthRounder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TargetQueueLengthRounder.class);

    private final int roundingMultiple;

    public TargetQueueLengthRounder(final int roundingMultiple) throws IllegalArgumentException {

        if (roundingMultiple == 0) {
            throw new IllegalArgumentException("Rounding multiple cannot be 0. Please set rounding multiple.");
        }

        this.roundingMultiple = roundingMultiple;
    }

    public long getRoundedTargetQueueLength(final long tunedTargetQueue) {

        LOGGER.debug("RoundingMultiple value has been set to: {}. This means any suggested target queues that are " +
                "not a multiple of {}, will be rounded to the nearest multiple.", roundingMultiple, roundingMultiple);

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
        LOGGER.debug("Target queue length has been rounded from: {} to: {}", tunedTargetQueue, roundedQueueLength);
        return roundedQueueLength;
    }
}

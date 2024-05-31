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

import com.github.workerframework.workermessageprioritization.targetqueue.TargetQueueLengthRounder;

import static org.hamcrest.CoreMatchers.equalTo;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

public class TargetQueueLengthRounderTest {

    @Test
    public void getRoundedTargetQueueLengthTest() {
        final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(100);

        Assertions.assertAll(() -> {
            final long roundedTargetQueueLength1 = targetQueueLengthRounder
                    .getRoundedTargetQueueLength(3000000);
            Assertions.assertEquals(3000000L,roundedTargetQueueLength1,
                    "Target queue length should not change as it is already a multiple of 100");

            final long roundedTargetQueueLength2 = targetQueueLengthRounder.getRoundedTargetQueueLength(50);
            Assertions.assertEquals(100L,roundedTargetQueueLength2,
                    "Target queue length should be rounded to 100");

            final long roundedTargetQueueLength3 = targetQueueLengthRounder.getRoundedTargetQueueLength(0);
            Assertions.assertEquals(0L, roundedTargetQueueLength3,
                    "Target queue length should be rounded to 0");

            final long roundedTargetQueueLength4 = targetQueueLengthRounder.getRoundedTargetQueueLength(561);
            Assertions.assertEquals(600L, roundedTargetQueueLength4,
                    "Target queue length should be rounded to the nearest 100");

            final long roundedTargetQueueLength5 = targetQueueLengthRounder.getRoundedTargetQueueLength(389);
            Assertions.assertEquals(400L, roundedTargetQueueLength5,
                    "Target queue length should be rounded to the nearest 100");

            final long roundedTargetQueueLength6 = targetQueueLengthRounder.getRoundedTargetQueueLength(237);
            Assertions.assertEquals(200L, roundedTargetQueueLength6,
                    "Target queue length should be rounded to the nearest 100");

            final long roundedTargetQueueLength7 = targetQueueLengthRounder.getRoundedTargetQueueLength(56984934);
            Assertions.assertEquals(56984900L, roundedTargetQueueLength7,
                    "Target queue length should be rounded to the nearest 100");

            final long roundedTargetQueueLength8 = targetQueueLengthRounder.getRoundedTargetQueueLength(200);
            Assertions.assertEquals(200L, roundedTargetQueueLength8,
                    "Target queue length should not change as it is already a multiple of 100");

            final long roundedTargetQueueLength9 = targetQueueLengthRounder.getRoundedTargetQueueLength(449);
            Assertions.assertEquals(400L, roundedTargetQueueLength9,
                    "Target queue length should be rounded to the nearest 100");

            final long roundedTargetQueueLength10 = targetQueueLengthRounder.getRoundedTargetQueueLength(749382);
            Assertions.assertEquals(749400L, roundedTargetQueueLength10,
                    "Target queue length should be rounded to the nearest 100");
        });
    }

    @Test
    @SuppressWarnings("ThrowableResultIgnored")
    public void roundingMultipleCannotBeSetToZeroTest() {

        assertThrows(IllegalArgumentException.class, () -> {
            final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(0);
        });
    }
}

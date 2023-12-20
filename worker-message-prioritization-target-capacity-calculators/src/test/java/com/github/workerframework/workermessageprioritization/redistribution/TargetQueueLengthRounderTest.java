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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThrows;

public class TargetQueueLengthRounderTest {

    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    @Test
    public void getRoundedTargetQueueLengthTest() {
        final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(100);

        final long roundedTargetQueueLength1 = targetQueueLengthRounder
                .getRoundedTargetQueueLength(3000000);
        collector.checkThat("Target queue length should not change as it is already a multiple of 100", 
                3000000L,
                equalTo(roundedTargetQueueLength1));

        final long roundedTargetQueueLength2 = targetQueueLengthRounder.getRoundedTargetQueueLength(50);
        collector.checkThat("Target queue length should be rounded to 100", 100L, 
                equalTo(roundedTargetQueueLength2));

        final long roundedTargetQueueLength3 = targetQueueLengthRounder.getRoundedTargetQueueLength(0);
        collector.checkThat("Target queue length should be rounded to 0", 0L, 
                equalTo(roundedTargetQueueLength3));

        final long roundedTargetQueueLength4 = targetQueueLengthRounder.getRoundedTargetQueueLength(561);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 600L, 
                equalTo(roundedTargetQueueLength4));

        final long roundedTargetQueueLength5 = targetQueueLengthRounder.getRoundedTargetQueueLength(389);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 400L, 
                equalTo(roundedTargetQueueLength5));

        final long roundedTargetQueueLength6 = targetQueueLengthRounder.getRoundedTargetQueueLength(237);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 200L, 
                equalTo(roundedTargetQueueLength6));

        final long roundedTargetQueueLength7 = targetQueueLengthRounder.getRoundedTargetQueueLength(56984934);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 56984900L, 
                equalTo(roundedTargetQueueLength7));

        final long roundedTargetQueueLength8 = targetQueueLengthRounder.getRoundedTargetQueueLength(200);
        collector.checkThat("Target queue length should not change as it is already a multiple of 100", 200L,
                equalTo(roundedTargetQueueLength8));

        final long roundedTargetQueueLength9 = targetQueueLengthRounder.getRoundedTargetQueueLength(449);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 400L, 
                equalTo(roundedTargetQueueLength9));

        final long roundedTargetQueueLength10 = targetQueueLengthRounder.getRoundedTargetQueueLength(749382);
        collector.checkThat("Target queue length should be rounded to the nearest 100", 749400L, 
                equalTo(roundedTargetQueueLength10));
    }

    @Test
    public void roundingMultipleCannotBeSetToZeroTest() {

        assertThrows(IllegalArgumentException.class, () -> {
            final TargetQueueLengthRounder targetQueueLengthRounder = new TargetQueueLengthRounder(0);
        });
    }
}

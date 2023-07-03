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
package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class RoundTargetQueueLengthTest {

    @Rule
    public final ErrorCollector collector = new ErrorCollector();

    @Test
    public void getRoundedTargetQueueLengthTest() {
        final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(100);

        final long roundedTargetQueueLength1 = roundTargetQueueLength.getRoundedTargetQueueLength(3000000);
        collector.checkThat("Target queue length should not change as it is already a multiple of 100",3000000L,
                equalTo(roundedTargetQueueLength1));

        final long roundedTargetQueueLength2 = roundTargetQueueLength.getRoundedTargetQueueLength(50);
        collector.checkThat("Target queue length should be rounded to 100", 100L, equalTo(roundedTargetQueueLength2));

        final long roundedTargetQueueLength3 = roundTargetQueueLength.getRoundedTargetQueueLength(0);
        collector.checkThat("Target queue length should be rounded to 0",0L, equalTo(roundedTargetQueueLength3));

        final long roundedTargetQueueLength4 = roundTargetQueueLength.getRoundedTargetQueueLength(561);
        collector.checkThat("Target queue length should be rounded to the nearest 100",600L, equalTo(roundedTargetQueueLength4));

        final long roundedTargetQueueLength5 = roundTargetQueueLength.getRoundedTargetQueueLength(389);
        collector.checkThat("Target queue length should be rounded to the nearest 100",400L, equalTo(roundedTargetQueueLength5));

        final long roundedTargetQueueLength6 = roundTargetQueueLength.getRoundedTargetQueueLength(237);
        collector.checkThat("Target queue length should be rounded to the nearest 100",200L, equalTo(roundedTargetQueueLength6));

        final long roundedTargetQueueLength7 = roundTargetQueueLength.getRoundedTargetQueueLength(56984934);
        collector.checkThat("Target queue length should be rounded to the nearest 100",56984900L, equalTo(roundedTargetQueueLength7));

        final long roundedTargetQueueLength8 = roundTargetQueueLength.getRoundedTargetQueueLength(200);
        collector.checkThat("Target queue length should not change as it is already a multiple of 100",200L,
                equalTo(roundedTargetQueueLength8));

        final long roundedTargetQueueLength9 = roundTargetQueueLength.getRoundedTargetQueueLength(449);
        collector.checkThat("Target queue length should be rounded to the nearest 100",400L, equalTo(roundedTargetQueueLength9));

        final long roundedTargetQueueLength10 = roundTargetQueueLength.getRoundedTargetQueueLength(749382);
        collector.checkThat("Target queue length should be rounded to the nearest 100",749400L, equalTo(roundedTargetQueueLength10));
    }


    @Test
    public void roundingMultipleCannotBeSetToZeroTest() {

        try {
            final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(0);
            fail();
        } catch (final IllegalArgumentException exception){
            assertNotNull("IllegalArgumentException was not thrown", exception);
            assertEquals("Rounding multiple cannot be 0. Please set rounding multiple.",
                    exception.getMessage());
        }
    }
}
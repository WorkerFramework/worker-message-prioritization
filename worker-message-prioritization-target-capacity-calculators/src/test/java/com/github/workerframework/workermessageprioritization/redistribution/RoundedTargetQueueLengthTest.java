package com.github.workerframework.workermessageprioritization.redistribution;

import com.github.workerframework.workermessageprioritization.targetqueue.RoundTargetQueueLength;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RoundedTargetQueueLengthTest {

    @Test
    public void getRoundedTargetQueueLengthTest() {
        final RoundTargetQueueLength roundTargetQueueLength = new RoundTargetQueueLength(100);
        final long roundedTargetQueueLength1 = roundTargetQueueLength.getRoundedTargetQueueLength(3000000);
        final long roundedTargetQueueLength2 = roundTargetQueueLength.getRoundedTargetQueueLength(50);
        final long roundedTargetQueueLength3 = roundTargetQueueLength.getRoundedTargetQueueLength(0);
        final long roundedTargetQueueLength4 = roundTargetQueueLength.getRoundedTargetQueueLength(561);
        final long roundedTargetQueueLength5 = roundTargetQueueLength.getRoundedTargetQueueLength(389);
        final long roundedTargetQueueLength6 = roundTargetQueueLength.getRoundedTargetQueueLength(237);
        final long roundedTargetQueueLength7 = roundTargetQueueLength.getRoundedTargetQueueLength(56984934);
        final long roundedTargetQueueLength8 = roundTargetQueueLength.getRoundedTargetQueueLength(200);
        final long roundedTargetQueueLength9 = roundTargetQueueLength.getRoundedTargetQueueLength(449);
        final long roundedTargetQueueLength10 = roundTargetQueueLength.getRoundedTargetQueueLength(749382);
        assertEquals( "Target queue length should not change as it is already a multiple of 100", 3000000,
                roundedTargetQueueLength1);
        assertEquals( "Target queue length should be rounded to 100", 100, roundedTargetQueueLength2);
        assertEquals( "Target queue length should be rounded to 0", 0, roundedTargetQueueLength3);
        assertEquals( "Target queue length should be rounded to the nearest 100", 600, roundedTargetQueueLength4);
        assertEquals( "Target queue length should be rounded to the nearest 100", 400, roundedTargetQueueLength5);
        assertEquals( "Target queue length should be rounded to the nearest 100", 200, roundedTargetQueueLength6);
        assertEquals( "Target queue length should be rounded to the nearest 100", 56984900, roundedTargetQueueLength7);
        assertEquals( "Target queue length should not change as it is already a multiple of 100", 200,
                roundedTargetQueueLength8);
        assertEquals( "Target queue length should be rounded to the nearest 100", 400, roundedTargetQueueLength9);
        assertEquals( "Target queue length should be rounded to the nearest 100", 749400, roundedTargetQueueLength10);
    }
}

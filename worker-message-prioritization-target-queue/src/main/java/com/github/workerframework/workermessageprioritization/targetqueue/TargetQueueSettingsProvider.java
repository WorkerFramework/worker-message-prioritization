package com.github.workerframework.workermessageprioritization.targetqueue;

import com.github.workerframework.workermessageprioritization.rabbitmq.Queue;
import com.github.workerframework.workermessageprioritization.targetqueue.targetqueuesettings.TargetQueueSettings;

public interface TargetQueueSettingsProvider
{

    /**
     * Obtain the settings for a target queue
     */
    TargetQueueSettings get(final Queue targetQueue);
}

package com.github.workerframework.workermessageprioritization.rabbitmq;

import com.google.common.base.Strings;

public final class RabbitQueueConstants {

    public static final String RABBIT_PROP_QUEUE_TYPE = "x-queue-type";
    public static final String RABBIT_PROP_QUEUE_TYPE_CLASSIC = "classic";
    public static final String RABBIT_PROP_QUEUE_TYPE_QUORUM = "quorum";
    public static final String RABBIT_PROP_QUEUE_TYPE_NAME = !Strings.isNullOrEmpty(System.getenv("RABBIT_PROP_QUEUE_TYPE_NAME"))?
            System.getenv("RABBIT_PROP_QUEUE_TYPE_NAME") : RABBIT_PROP_QUEUE_TYPE_CLASSIC;


    private RabbitQueueConstants() {
    }
}

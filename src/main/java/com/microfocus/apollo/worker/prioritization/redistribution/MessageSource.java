package com.microfocus.apollo.worker.prioritization.redistribution;

import com.microfocus.apollo.worker.prioritization.management.Queue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSource.class);
    private final ConnectionFactory connectionFactory;
    private Channel incomingChannel;
    private Channel outgoingChannel;
    private final MessageTarget messageTarget;
    private final Queue sourceQueue;
    private ShutdownSignalException shutdownSignalException = null;
    private final ConcurrentNavigableMap<Long, Long> outstandingConfirms = new ConcurrentSkipListMap<>();

    private boolean active;
    public MessageSource(final ConnectionFactory connectionFactory, final Queue sourceQueue, final MessageTarget messageTarget) {
        this.connectionFactory = connectionFactory;
        this.messageTarget = messageTarget;
        this.sourceQueue = sourceQueue;
    }
    
    public void init() throws IOException, TimeoutException {
        this.incomingChannel = connectionFactory.newConnection().createChannel();
        this.incomingChannel.basicQos(1);
        this.outgoingChannel = connectionFactory.newConnection().createChannel();
        registerConfirmListener();
    }
    
    private void registerConfirmListener() throws IOException {
        outgoingChannel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );
                    
                    LOGGER.info("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                            confirmed.lastKey(), sourceQueue.getName(), outstandingConfirms.get(deliveryTag), 
                            messageTarget.getTargetQueueName());

//                    incomingChannel.basicAck(confirmed.lastKey(), true);
                    confirmed.clear();
                } else {
                    LOGGER.info("Ack message source delivery {} from {} after publish confirm {} of message to {}",
                            outstandingConfirms.get(deliveryTag), sourceQueue.getName(), outstandingConfirms.get(deliveryTag),
                            messageTarget.getTargetQueueName());
                    
//                    incomingChannel.basicAck(outstandingConfirms.get(deliveryTag), false);
                    outstandingConfirms.remove(deliveryTag);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                if (multiple) {
                    final ConcurrentNavigableMap<Long, Long> confirmed = outstandingConfirms.headMap(
                            deliveryTag, true
                    );

                    for(final Long messageDeliveryTagToNack: confirmed.values()) {
                        try {
                            incomingChannel.basicNack(messageDeliveryTagToNack, true, true);
                        }
                        catch (final IOException e) {
                            //TODO Consider allowing a retry limit before escalating and removing this messageSource
                            LOGGER.error("Exception ack'ing '{}' {}",
                                    messageDeliveryTagToNack,
                                    e.toString());
                        }
                    }

                    confirmed.clear();
                } else {
                    incomingChannel.basicNack(outstandingConfirms.get(deliveryTag), false, true);
                    outstandingConfirms.remove(deliveryTag);
                }
            }
        });
        outgoingChannel.confirmSelect();        
    }

    public void run(final long consumptionLimit) {

        if(active) {
            LOGGER.info("Source queue '{}' is still active, ignoring wireup request.", sourceQueue.getName());
            return;
        }

        try {
            final AtomicInteger messageCount = new AtomicInteger(0);
            incomingChannel.basicConsume(sourceQueue.getName(),
                    (consumerTag, message) -> {

                        final Envelope envelope = message.getEnvelope();
                        final long nextPublishSequenceNumber = outgoingChannel.getNextPublishSeqNo();
                        try {
                            LOGGER.info("Publishing message source {} from {} to {} and expecting publish confirm {}",
                                    envelope.getDeliveryTag(), sourceQueue.getName(), 
                                    messageTarget.getTargetQueueName(), nextPublishSequenceNumber);

                            outstandingConfirms.put(nextPublishSequenceNumber, envelope.getDeliveryTag());
                            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                                    .contentType(message.getProperties().getContentType())
                                    .deliveryMode(message.getProperties().getDeliveryMode())
                                    .priority(message.getProperties().getPriority())
                                    .build();
                            outgoingChannel.basicPublish("",
                                    messageTarget.getTargetQueueName(), basicProperties, message.getBody());
                        }
                        catch (final IOException e) {
                            LOGGER.error("Exception publishing to '{}' {}", messageTarget.getTargetQueueName(),
                                    e.toString());
                            //TODO Consider allowing a retry limit before escalating and stopping this MessageTarget
                            outstandingConfirms.remove(nextPublishSequenceNumber);
//                            incomingChannel.basicCancel(consumerTag);
                        }
                        
                        messageCount.incrementAndGet();

                        if(messageCount.get() > consumptionLimit) {
                            LOGGER.info("Consumption target '{}' reached for '{}'.", consumptionLimit,
                                    sourceQueue.getName());
                            incomingChannel.basicCancel(consumerTag);
                        }
                    },
                    consumerTag -> {
                        //Stop tracking that we are consuming from the consumerTag queue
                        active = false;
                    },
                    (consumerTag, sig) -> {
                        //Connection lost, give up
                        shutdownSignalException = sig;
                        active = false;
                    });
            active = true;
        } catch (final IOException e) {
            LOGGER.error("Exception registering MessageSource from '{}' queue", sourceQueue.getName());
            active = false;
        }

    }

    public ShutdownSignalException getShutdownSignalException() {
        return shutdownSignalException;
    }
    
    public Queue getSourceQueue() {
        return sourceQueue;
    }
}

package com.loiko.alex.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public interface ConsumerUtils {

    @Slf4j
    final class LogHolder {
    }

    static Consumer<byte[]> createConsumer(PulsarClient pulsarClient, SubscriptionType subscriptionType) throws PulsarClientException {
        return pulsarClient.newConsumer()
                .topic(PulsarConstants.TOPIC)
                .subscriptionName(PulsarConstants.SUBSCRIPTION_NAME)
                .subscriptionType(subscriptionType) //SubscriptionType.Exclusive by default
                .subscribe();
    }

    static Consumer<byte[]> createConsumerWithDLQPolicy(PulsarClient pulsarClient, SubscriptionType subscriptionType) throws PulsarClientException {
        return pulsarClient.newConsumer()
                .topic(PulsarConstants.TOPIC)
                .subscriptionName(PulsarConstants.SUBSCRIPTION_NAME)
                .subscriptionType(subscriptionType)
                .deadLetterPolicy(DeadLetterPolicy.builder()
                        .maxRedeliverCount(5)
                        .deadLetterTopic(PulsarConstants.DLQ_TOPIC).build())
                .subscribe();
    }

    static ConsumerBuilder<byte[]> createConsumerBuilderWithSharedSubscriptionTypeAndMessageListener(PulsarClient pulsarClient) {
        return pulsarClient.newConsumer()
                .topic(PulsarConstants.TOPIC)
                .subscriptionName(PulsarConstants.SUBSCRIPTION_NAME)
                .subscriptionType(SubscriptionType.Shared)
                .consumerName("default-consumer-name")
                .messageListener((consumer, message) -> { //non-blocking
                    try {
                        LogHolder.log.info("{} received message: {}", consumer.getConsumerName(), new String(message.getData()));
                        consumer.acknowledge(message);
                    } catch (PulsarClientException exception) {
                        exception.printStackTrace();
                    }
                });
    }

    static List<Consumer<byte[]>> subscribeConsumers(ConsumerBuilder<byte[]> consumerBuilder) {
        List<Consumer<byte[]>> consumers = new ArrayList<>();
        IntStream.range(0, 4).forEach(i -> {
            String name = String.format("mq-consumer-%d", i);
            try {
                Consumer<byte[]> consumer = consumerBuilder.consumerName(name).subscribe();
                consumers.add(consumer);
            } catch (PulsarClientException exception) {
                exception.printStackTrace();
            }
        });
        return consumers;
    }

    static void closeConsumer(Consumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (PulsarClientException exception) {
            exception.printStackTrace();
        }
    }

    static void syncConsume(Consumer<byte[]> consumer) {
        while (true) {
            try {
                Message<byte[]> message = consumer.receive(); //blocking
                LogHolder.log.info("Message received: {}", new String(message.getData()));
                acknowledgeMessage(consumer, message);
            } catch (Exception exception) {
                return;
            }
        }
    }

    private static void acknowledgeMessage(Consumer<byte[]> consumer, Message<byte[]> message) {
        try {
            consumer.acknowledge(message);
        } catch (Exception exception) {
            consumer.negativeAcknowledge(message);
        }
    }
}

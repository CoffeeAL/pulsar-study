package com.loiko.alex.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;

public interface ProducerUtils {

    @Slf4j
    final class LogHolder {}

    static Producer<byte[]> create(PulsarClient pulsarClient, String topic) throws PulsarClientException {
        return pulsarClient.newProducer()
                .topic(topic)
                .create();
    }

    static void sendMessage(Producer<byte[]> producer) {
        List.of("Red", "Yellow", "Green").forEach(color -> send(producer, color));
        try {
            producer.close();
            LogHolder.log.info("Producer closed");
        } catch (PulsarClientException exception) {
            exception.printStackTrace();
        }
    }

    private static void send(Producer<byte[]> producer, String color) {
        try {
            producer.newMessage()
                    .key("color")
                    .value(color.getBytes())
                    .property("id", "1")
                    .property("day", "08.10.2024")
                    .send();
            LogHolder.log.info("Message sent for color: {}", color);
            Thread.sleep(3000);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}

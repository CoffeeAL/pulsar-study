package com.loiko.alex.runner;

import com.loiko.alex.utils.ConsumerUtils;
import com.loiko.alex.utils.ProducerUtils;
import com.loiko.alex.utils.PulsarAdminUtils;
import com.loiko.alex.utils.PulsarClientUtils;
import com.loiko.alex.utils.PulsarConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Date;

@Slf4j
public class ProducerConsumerRunner {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {
        try (PulsarAdmin pulsarAdmin = PulsarAdminUtils.create(PulsarConstants.PULSAR_ADMIN_URL)) {
            //TODO provide clearing data while stopping this infinite loop
            PulsarAdminUtils.clear(pulsarAdmin);
            PulsarAdminUtils.administrate(pulsarAdmin);

            startProducer();
            startConsumer();
        }
    }

    private static void startProducer() {
        Runnable runnable = () -> {
            int counter = 0;
            while (true) {
                try {
                    getProducer().newMessage()
                            .value(String.format("{id: %d, time: %tc}", ++counter, new Date()).getBytes())
                            .send();
                    Thread.sleep(1000L);
                } catch (PulsarClientException | InterruptedException exception) {
                    exception.printStackTrace();
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    private static Producer<byte[]> getProducer() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientUtils.create(PulsarConstants.PULSAR_CLIENT_URL);
        return ProducerUtils.create(pulsarClient, PulsarConstants.TOPIC);
    }

    private static void startConsumer() {
        Runnable runnable = () -> {
            while (true) {
                Message<byte[]> message;
                try {
                    Consumer<byte[]> consumer = getConsumer();
                    message = consumer.receive();
                    log.info("Message received: {}", new String(message.getData()));
                    consumer.acknowledge(message);
                } catch (PulsarClientException exception) {
                    exception.printStackTrace();
                }
            }
        };
        Thread thread = new Thread(runnable);
        thread.start();
    }

    private static Consumer<byte[]> getConsumer() throws PulsarClientException {
        PulsarClient pulsarClient = PulsarClientUtils.create(PulsarConstants.PULSAR_CLIENT_URL);
        return ConsumerUtils.createConsumer(pulsarClient, SubscriptionType.Shared);
    }
}

package com.loiko.alex;

import com.loiko.alex.utils.PulsarConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class PulsarDemo {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException, InterruptedException {
        try (PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(PulsarConstants.PULSAR_ADMIN_URL).build()) {
            administrate(pulsarAdmin);
            PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PulsarConstants.PULSAR_CLIENT_URL).build();

            Consumer<byte[]> consumer = createConsumer(pulsarClient);
            Thread consumerThread = new Thread(() -> syncConsume(consumer));
            consumerThread.start();

            Thread.sleep(5000);

            Producer<byte[]> producer = createProducer(pulsarClient);
            Thread producerThread = new Thread(() -> sendMessage(producer));
            producerThread.start();

            producerThread.join();
            consumerThread.interrupt();
            consumerThread.join();
            consumer.close();
            log.info("Consumer closed");

            clear(pulsarAdmin);
        }
    }

    private static void administrate(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        TenantInfo config = TenantInfo.builder()
                .adminRoles(Stream.of(PulsarConstants.ROLE).collect(Collectors.toCollection(HashSet::new)))
                .allowedClusters(Stream.of(PulsarConstants.CLUSTER).collect(Collectors.toCollection(HashSet::new)))
                .build();

        pulsarAdmin.tenants().createTenant(PulsarConstants.TENANT, config);
        pulsarAdmin.namespaces().createNamespace(PulsarConstants.NAMESPACE);
        pulsarAdmin.topics().createNonPartitionedTopic(PulsarConstants.TOPIC);
    }

    private static Consumer<byte[]> createConsumer(PulsarClient pulsarClient) throws PulsarClientException {
        return pulsarClient.newConsumer()
                .topic(PulsarConstants.TOPIC)
                .subscriptionName(PulsarConstants.SUBSCRIPTION_NAME)
                .subscribe();
    }

    private static void syncConsume(Consumer<byte[]> consumer) {
        while (true) {
            try {
                Message<byte[]> message = consumer.receive();
                log.info("Message received: {}", new String(message.getData()));
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

    private static Producer<byte[]> createProducer(PulsarClient pulsarClient) throws PulsarClientException {
        return pulsarClient.newProducer()
                .topic(PulsarConstants.TOPIC)
                .create();
    }

    private static void sendMessage(Producer<byte[]> producer) {
        List.of("Red", "Yellow", "Green").forEach(color -> send(producer, color));
        try {
            producer.close();
            log.info("Producer closed");
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
            log.info("Message sent for color: {}", color);
            Thread.sleep(3000);
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    private static void clear(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        log.info("Clearing data...");
        pulsarAdmin.topics().delete(PulsarConstants.TOPIC);
        pulsarAdmin.namespaces().deleteNamespace(PulsarConstants.NAMESPACE);
        pulsarAdmin.tenants().deleteTenant(PulsarConstants.TENANT);
    }
}

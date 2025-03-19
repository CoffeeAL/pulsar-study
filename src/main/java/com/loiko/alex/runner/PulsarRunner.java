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
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

@Slf4j
public class PulsarRunner {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException, InterruptedException {
        try (PulsarAdmin pulsarAdmin = PulsarAdminUtils.create(PulsarConstants.PULSAR_ADMIN_URL)) {
            PulsarAdminUtils.administrate(pulsarAdmin);
            PulsarClient pulsarClient = PulsarClientUtils.create(PulsarConstants.PULSAR_CLIENT_URL);

            Consumer<byte[]> consumer = ConsumerUtils.createConsumer(pulsarClient, SubscriptionType.Exclusive);
            Thread consumerThread = new Thread(() -> ConsumerUtils.syncConsume(consumer));
            consumerThread.start();

            Thread.sleep(5000);

            Producer<byte[]> producer = ProducerUtils.create(pulsarClient, PulsarConstants.TOPIC);
            Thread producerThread = new Thread(() -> ProducerUtils.sendMessage(producer));
            producerThread.start();

            producerThread.join();
            consumerThread.interrupt();
            consumerThread.join();
            consumer.close();
            log.info("Consumer closed");

            PulsarAdminUtils.clear(pulsarAdmin);
        }
    }
}

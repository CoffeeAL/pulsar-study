package com.loiko.alex.runner;

import com.loiko.alex.utils.ConsumerUtils;
import com.loiko.alex.utils.ProducerUtils;
import com.loiko.alex.utils.PulsarAdminUtils;
import com.loiko.alex.utils.PulsarClientUtils;
import com.loiko.alex.utils.PulsarConstants;
import com.loiko.alex.utils.ReaderUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;

import java.io.IOException;
import java.util.List;

@Slf4j
public class ReaderRunner {

    public static void main(String[] args) throws PulsarAdminException, IOException, InterruptedException {
        try (PulsarAdmin pulsarAdmin = PulsarAdminUtils.create(PulsarConstants.PULSAR_ADMIN_URL)) {
            PulsarAdminUtils.administrate(pulsarAdmin);
            PulsarClient pulsarClient = PulsarClientUtils.create(PulsarConstants.PULSAR_CLIENT_URL);

            Producer<byte[]> producer = ProducerUtils.create(pulsarClient, PulsarConstants.TOPIC);
            Thread producerThread = new Thread(() -> ProducerUtils.sendMessage(producer));
            producerThread.start();

            Thread.sleep(5000);

            //for comparing outputs for consumers and reader
            ConsumerBuilder<byte[]> consumerBuilder = ConsumerUtils.createConsumerBuilderWithSharedSubscriptionTypeAndMessageListener(pulsarClient);
            List<Consumer<byte[]>> consumers = ConsumerUtils.subscribeConsumers(consumerBuilder);

            Reader<byte[]> reader = ReaderUtils.create(pulsarClient);
            ReaderUtils.read(reader);

            producerThread.join();

            consumers.forEach(ConsumerUtils::closeConsumer);
            PulsarAdminUtils.clear(pulsarAdmin);
        }
    }
}

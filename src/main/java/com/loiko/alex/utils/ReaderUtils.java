package com.loiko.alex.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

import java.io.IOException;

public interface ReaderUtils {

    @Slf4j
    final class LogHolder {}

    static Reader<byte[]> create(PulsarClient pulsarClient) throws PulsarClientException {
        return pulsarClient.newReader()
                .topic(PulsarConstants.TOPIC)
                .readerName(PulsarConstants.READER_NAME)
                .startMessageId(MessageId.earliest)
                .create();
    }

    static void read(Reader<byte[]> reader) throws IOException {
        long now = System.currentTimeMillis();
        long expired = now + 30000L;
        while(now < expired) {
            Message<byte[]> message = reader.readNext();
            LogHolder.log.info("Message received by reader: {}", new String(message.getData()));
            now = System.currentTimeMillis();
        }
        reader.close();
    }
}

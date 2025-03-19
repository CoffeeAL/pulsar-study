package com.loiko.alex.utils;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public interface PulsarClientUtils {

    static PulsarClient create(String serviceUrl) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(serviceUrl).build();
    }
}

package com.loiko.alex.utils;

public interface PulsarConstants {

    String PULSAR_ADMIN_URL = "http://localhost:8080";
    String PULSAR_CLIENT_URL = "pulsar://localhost:6650";

    String PERSISTENT_DOMAIN = "persistent://";
    String TENANT = "my-tenant";
    String NAMESPACE = TENANT + "/my-namespace";
    String TOPIC = PERSISTENT_DOMAIN + NAMESPACE + "/my-topic";

    String ROLE = "admin";
    String CLUSTER = "standalone";

    String SUBSCRIPTION_NAME = "my-subscription";
}

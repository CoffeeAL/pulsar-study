package com.loiko.alex.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface PulsarAdminUtils {

    @Slf4j
    final class LogHolder {}

    static PulsarAdmin create(String serviceHttpUrl) throws PulsarClientException {
        return PulsarAdmin.builder().serviceHttpUrl(serviceHttpUrl).build();
    }

    static void administrate(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        TenantInfo config = TenantInfo.builder()
                .adminRoles(Stream.of(PulsarConstants.ROLE).collect(Collectors.toCollection(HashSet::new)))
                .allowedClusters(Stream.of(PulsarConstants.CLUSTER).collect(Collectors.toCollection(HashSet::new)))
                .build();

        pulsarAdmin.tenants().createTenant(PulsarConstants.TENANT, config);
        pulsarAdmin.namespaces().createNamespace(PulsarConstants.NAMESPACE);
        pulsarAdmin.topics().createNonPartitionedTopic(PulsarConstants.TOPIC);
        pulsarAdmin.topics().createNonPartitionedTopic(PulsarConstants.DLQ_TOPIC);
    }

    static void clear(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        LogHolder.log.info("Clearing data...");
        pulsarAdmin.topics().delete(PulsarConstants.TOPIC);
        pulsarAdmin.topics().delete(PulsarConstants.DLQ_TOPIC);
        pulsarAdmin.namespaces().deleteNamespace(PulsarConstants.NAMESPACE);
        pulsarAdmin.tenants().deleteTenant(PulsarConstants.TENANT);
    }
}

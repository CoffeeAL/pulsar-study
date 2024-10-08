package com.loiko.alex;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarDemo {

    public static void main(String[] args) throws PulsarClientException, PulsarAdminException {

        try (PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build()) {
            TenantInfo config = TenantInfo.builder()
                    .adminRoles(Stream.of("admin").collect(Collectors.toCollection(HashSet::new)))
                    .allowedClusters(Stream.of("standalone").collect(Collectors.toCollection(HashSet::new)))
                    .build();

            pulsarAdmin.tenants().createTenant("my-tenant-2", config);
            pulsarAdmin.namespaces().createNamespace("my-tenant-2/test-namespace");
            pulsarAdmin.topics().createNonPartitionedTopic("persistent://my-tenant-2/test-namespace/example-topic");
            System.out.println("Done");
        }
    }
}

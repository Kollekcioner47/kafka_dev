package dev.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Admin API (Модуль 4).
 * Режимы: create, describe, partitions, groups, delete-records, reset-offsets,
 *         quota, list-quotas, acl, list-acls.
 */
public class AdminExample {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "create";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

        try (AdminClient admin = AdminClient.create(props)) {
            switch (mode) {
                case "create" -> create(admin);
                case "describe" -> describe(admin);
                case "partitions" -> partitions(admin);
                case "groups" -> groups(admin);
                case "delete-records" -> deleteRecords(admin);
                case "reset-offsets" -> resetOffsets(admin);
                case "quota" -> quota(admin);
                case "list-quotas" -> listQuotas(admin);
                case "acl" -> acl(admin);
                case "list-acls" -> listAcls(admin);
                default -> System.out.println("unknown mode: " + mode);
            }
        }
    }

    private static void create(AdminClient admin) throws Exception {
        Set<String> existing = admin.listTopics().names().get();
        if (!existing.contains("orders")) {
            admin.createTopics(List.of(new NewTopic("orders", 4, (short) 1))).all().get();
        }
        if (!existing.contains("orders-agg")) {
            admin.createTopics(List.of(new NewTopic("orders-agg", 4, (short) 1))).all().get();
        }
        System.out.println("topics ready: " + admin.listTopics().names().get());
    }

    private static void describe(AdminClient admin) throws Exception {
        Map<String, TopicDescription> desc =
                admin.describeTopics(List.of("orders", "orders-agg")).allTopicNames().get();
        desc.forEach((name, d) -> d.partitions().forEach(p ->
                System.out.printf("%s p%d leader=%d isr=%s%n",
                        name, p.partition(), p.leader().id(), p.isr())));

        ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        Config cfg = admin.describeConfigs(List.of(r)).all().get().get(r);
        ConfigEntry retention = cfg.get("retention.ms");
        System.out.println("orders retention.ms=" +
                (retention != null ? retention.value() : "?"));
    }

    private static void partitions(AdminClient admin) throws Exception {
        int current = admin.describeTopics(List.of("orders"))
                .allTopicNames().get().get("orders").partitions().size();
        if (current < 6) {
            admin.createPartitions(Map.of("orders", NewPartitions.increaseTo(6))).all().get();
            System.out.println("orders: " + current + " -> 6 partitions");
        } else {
            System.out.println("orders already has " + current + " partitions");
        }
    }

    private static void groups(AdminClient admin) throws Exception {
        admin.listConsumerGroups().all().get().forEach(g ->
                System.out.println("group=" + g.groupId()
                        + " state=" + g.state().map(Enum::toString).orElse("?")));

        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets("orders-group").partitionsToOffsetAndMetadata().get();
        offsets.forEach((tp, om) -> System.out.println(tp + " offset=" + om.offset()));
    }

    private static void deleteRecords(AdminClient admin) throws Exception {
        // Удалить сообщения партиции 0 до offset 11 (не включая 11).
        admin.deleteRecords(Map.of(
                new TopicPartition("orders", 0), RecordsToDelete.beforeOffset(11L)))
                .all().get();
        System.out.println("orders p0: records before offset 11 deleted");
    }

    private static void resetOffsets(AdminClient admin) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> current =
                admin.listConsumerGroupOffsets("orders-group").partitionsToOffsetAndMetadata().get();

        // OffsetSpec (earliest/latest/timestamp) — это не готовый offset, а «запрос»;
        // его нужно резолвить через listOffsets(), и только затем передавать
        // в alterConsumerGroupOffsets() как OffsetAndMetadata.
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        for (TopicPartition tp : current.keySet()) {
            specs.put(tp, OffsetSpec.earliest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                admin.listOffsets(specs).all().get();

        Map<TopicPartition, OffsetAndMetadata> reset = new HashMap<>();
        for (TopicPartition tp : current.keySet()) {
            reset.put(tp, new OffsetAndMetadata(earliest.get(tp).offset()));
        }
        admin.alterConsumerGroupOffsets("orders-group", reset).all().get();
        System.out.println("orders-group offsets reset to earliest");
    }

    private static void quota(AdminClient admin) throws Exception {
        admin.alterClientQuotas(List.of(new ClientQuotaAlteration(
                new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "heavy-client")),
                List.of(
                        new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0 * 1024.0),
                        new ClientQuotaAlteration.Op("consumer_byte_rate", 1024.0 * 1024.0)))))
                .all().get();
        System.out.println("quota set: heavy-client -> 1 MB/s");
    }

    private static void listQuotas(AdminClient admin) throws Exception {
        admin.describeClientQuotas(ClientQuotaFilter.all()).entities().get()
                .forEach((entity, quotas) -> System.out.println(entity + " -> " + quotas));
    }

    private static void acl(AdminClient admin) throws Exception {
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        admin.createAcls(List.of(binding)).all().get();
        System.out.println("ACL created: User:alice READ on topic orders");
    }

    private static void listAcls(AdminClient admin) throws Exception {
        admin.describeAcls(AclBindingFilter.ANY).values().get().forEach(System.out::println);
    }
}

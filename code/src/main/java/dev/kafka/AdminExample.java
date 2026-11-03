package dev.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
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
 *
 * ЧТО: показывает администрирование Kafka через {@link AdminClient} —
 *      программный аналог CLI-утилит kafka-topics / kafka-consumer-groups /
 *      kafka-acls / kafka-configs.
 *
 * ЗАЧЕМ: в реальных проектах топики, квоты и ACL создают не руками в терминале,
 *        а кодом — при развёртывании сервиса, в миграциях, в самообслуживании
 *        команд. AdminClient даёт те же операции, что и CLI, но из Java.
 *
 * КАК: main() читает первый аргумент (режим) и вызывает нужный метод. Все
 *      операции синхронные — вызов .get() блокирует поток до ответа брокера.
 *
 * Режимы: create | describe | partitions | groups | delete-records |
 *         reset-offsets | quota | list-quotas | acl | list-acls
 */
public class AdminExample {

    public static void main(String[] args) throws Exception {
        // Режим по умолчанию — create, чтобы запуск «без аргументов» что-то делал.
        String mode = args.length > 0 ? args[0] : "create";

        Properties props = new Properties();
        // Три брокера: bootstrap перечисляет все три, чтобы клиент стартовал,
        // даже если какой-то один брокер временно недоступен (остальные он
        // узнает из метаданных кластера).
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        // AdminClient потокобезопасен; закрываем через try-with-resources,
        // чтобы освободить соединения и фоновые потоки.
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

    /**
     * Создаёт топики orders и orders-agg.
     *
     * NewTopic(имя, число партиций, replication factor):
     *   - 4 партиции — параллелизм чтения/записи;
     *   - RF = 3 — каждая партиция хранится на ТРЁХ брокерах (у нас kafka1..3),
     *     т.е. отказоустойчивость: при падении одного брокера данные не теряются.
     *     Это НЕ «три топика», а три реплики каждой партиции.
     */
    private static void create(AdminClient admin) throws Exception {
        // Идемпотентность: сначала смотрим, что уже есть, чтобы при повторном
        // запуске не упасть с TopicExistsException.
        Set<String> existing = admin.listTopics().names().get();
        if (!existing.contains("orders")) {
            admin.createTopics(List.of(new NewTopic("orders", 4, (short) 3))).all().get();
        }
        if (!existing.contains("orders-agg")) {
            admin.createTopics(List.of(new NewTopic("orders-agg", 4, (short) 3))).all().get();
        }
        System.out.println("topics ready: " + admin.listTopics().names().get());
    }

    /**
     * Описывает топики: лидер и реплики каждой партиции + конфиг retention.ms.
     *
     * leader — брокер, обслуживающий запись/чтение данной партиции;
     * isr    — реплики, синхронные с лидером (in-sync replicas).
     * При трёх брокерах лидеры партиций распределены между kafka1/kafka2/kafka3.
     */
    private static void describe(AdminClient admin) throws Exception {
        Map<String, TopicDescription> desc =
                admin.describeTopics(List.of("orders", "orders-agg")).allTopicNames().get();
        desc.forEach((name, d) -> d.partitions().forEach(p ->
                System.out.printf("%s p%d leader=%d isr=%s%n",
                        name, p.partition(), p.leader().id(), p.isr())));

        // Конфигурация топика: сколько хранить данные (retention.ms).
        ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, "orders");
        Config cfg = admin.describeConfigs(List.of(r)).all().get().get(r);
        ConfigEntry retention = cfg.get("retention.ms");
        System.out.println("orders retention.ms=" +
                (retention != null ? retention.value() : "?"));
    }

    /**
     * Увеличивает число партиций топика orders с 4 до 6.
     *
     * ЗАЧЕМ осторожно: число партиций можно ТОЛЬКО увеличивать (уменьшение не
     * поддерживается). Изменение числа партиций меняет маршрутизацию ключей
     * (partition = hash(key) % N), поэтому уже записанные сообщения «разъедутся»
     * — на проде это редкая и продуманная операция.
     */
    private static void partitions(AdminClient admin) throws Exception {
        int current = admin.describeTopics(List.of("orders"))
                .allTopicNames().get().get("orders").partitions().size();
        if (current < 6) {
            // NewPartitions.increaseTo — увеличить до указанного числа.
            admin.createPartitions(Map.of("orders", NewPartitions.increaseTo(6))).all().get();
            System.out.println("orders: " + current + " -> 6 partitions");
        } else {
            System.out.println("orders already has " + current + " partitions");
        }
    }

    /**
     * Показывает consumer groups и закоммиченные офсеты группы orders-group.
     *
     * listConsumerGroupOffsets возвращает, ДО КАКОГО offset группа дочитала по
     * каждой партиции — это «закладка», с которой консюмер продолжит после
     * рестарта. По ней же считают lag (отставание).
     */
    private static void groups(AdminClient admin) throws Exception {
        admin.listConsumerGroups().all().get().forEach(g ->
                System.out.println("group=" + g.groupId()
                        + " state=" + g.state().map(Enum::toString).orElse("?")));

        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets("orders-group").partitionsToOffsetAndMetadata().get();
        offsets.forEach((tp, om) -> System.out.println(tp + " offset=" + om.offset()));
    }

    /**
     * Удаляет сообщения партиции 0 до offset 11 (не включая 11).
     *
     * ЗАЧЕМ: это низкоуровневая «чистка» данных (например, по требованию GDPR —
     * удалить конкретный диапазон). ВАЖНО: удаляет данные, но НЕ трогает
     * закоммиченные офсеты консюмеров — они продолжат читать «с прежнего места».
     */
    private static void deleteRecords(AdminClient admin) throws Exception {
        admin.deleteRecords(Map.of(
                new TopicPartition("orders", 0), RecordsToDelete.beforeOffset(11L)))
                .all().get();
        System.out.println("orders p0: records before offset 11 deleted");
    }

    /**
     * Перематывает группу orders-group на начало топика (earliest).
     *
     * ПОЧЕМУ так сложно: OffsetSpec (earliest/latest/timestamp) — это не готовое
     * число, а «запрос» к брокеру. Его сначала надо РЕЗОЛВИТЬ через listOffsets()
     * в конкретный offset, и только затем передать OffsetAndMetadata в
     * alterConsumerGroupOffsets(). Просто передать OffsetSpec нельзя — будет
     * ошибка компиляции/типов.
     *
     * Требование: группа должна быть БЕЗ активных консюмеров, иначе
     * alterConsumerGroupOffsets упадёт с UnknownMemberIdException.
     */
    private static void resetOffsets(AdminClient admin) throws Exception {
        // Текущие офсеты группы — узнаём, какие партиции вообще есть.
        Map<TopicPartition, OffsetAndMetadata> current =
                admin.listConsumerGroupOffsets("orders-group").partitionsToOffsetAndMetadata().get();

        // Шаг 1: для каждой партиции — «запрос» на earliest-offset.
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        for (TopicPartition tp : current.keySet()) {
            specs.put(tp, OffsetSpec.earliest());
        }
        // Шаг 2: резолвим запросы в конкретные числа.
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                admin.listOffsets(specs).all().get();

        // Шаг 3: собираем OffsetAndMetadata и применяем к группе.
        Map<TopicPartition, OffsetAndMetadata> reset = new HashMap<>();
        for (TopicPartition tp : current.keySet()) {
            reset.put(tp, new OffsetAndMetadata(earliest.get(tp).offset()));
        }
        admin.alterConsumerGroupOffsets("orders-group", reset).all().get();
        System.out.println("orders-group offsets reset to earliest");
    }

    /**
     * Задаёт квоту 1 МБ/с на запись и чтение для client.id "heavy-client".
     *
     * ЗАЧЕМ: квоты защищают кластер от «шумного соседа» — один продюсер, качающий
     * гигабайты, не должен задушить остальных. Квота вешается по client.id, user
     * (SASL) или их паре.
     */
    private static void quota(AdminClient admin) throws Exception {
        admin.alterClientQuotas(List.of(new ClientQuotaAlteration(
                new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "heavy-client")),
                List.of(
                        new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0 * 1024.0),
                        new ClientQuotaAlteration.Op("consumer_byte_rate", 1024.0 * 1024.0)))))
                .all().get();
        System.out.println("quota set: heavy-client -> 1 MB/s");
    }

    /** Показывает все установленные квоты. */
    private static void listQuotas(AdminClient admin) throws Exception {
        admin.describeClientQuotas(ClientQuotaFilter.all()).entities().get()
                .forEach((entity, quotas) -> System.out.println(entity + " -> " + quotas));
    }

    /**
     * Создаёт ACL: разрешить пользователю alice ЧИТАТЬ топик orders.
     *
     * ACL — это пара:
     *   ResourcePattern — НА ЧТО (тип ресурса TOPIC + имя "orders");
     *   AccessControlEntry — КТО (User:alice) и ЧТО МОЖНО (READ, ALLOW).
     *
     * ВАЖНО: ACL работают, только если на брокере включён authorizer
     * (kafka.security.authorizer.AclAuthorizer). В нашем compose он по умолчанию
     * выключен — см. пояснение в Модуле 4.
     */
    private static void acl(AdminClient admin) throws Exception {
        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
        admin.createAcls(List.of(binding)).all().get();
        System.out.println("ACL created: User:alice READ on topic orders");
    }

    /** Показывает все ACL кластера. */
    private static void listAcls(AdminClient admin) throws Exception {
        admin.describeAcls(AclBindingFilter.ANY).values().get().forEach(System.out::println);
    }
}

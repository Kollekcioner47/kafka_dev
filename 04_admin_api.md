# Модуль 4. Admin API

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Управлять топиками (создание, описание, удаление, конфигурация).
- Управлять партициями (увеличение количества) и сообщениями (удаление до offset).
- Управлять консюмерами и группами (описание, сброс офсетов).
- Управлять ACL и квотами.
- Применить `AdminClient` для автоматизации администрирования.

---

## 1. Класс AdminClient

`AdminClient` — программный аналог CLI `kafka-topics`, `kafka-consumer-groups`,
`kafka-acls`, `kafka-configs`. Создание:

```java
import org.apache.kafka.clients.admin.*;
import java.util.Properties;

Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");

try (AdminClient admin = AdminClient.create(props)) {
    // ... операции
}
```

Почти все методы возвращают `*Result`, из которого берутся `KafkaFuture`-значения
(`.all().get()` — дождаться завершения). `AdminClient` потокобезопасен.

---

## 2. Управление топиками

```java
// Создание: имя -> число партиций и RF (у нас три брокера — RF=3)
NewTopic topic = new NewTopic("orders", 4, (short) 3);
admin.createTopics(List.of(topic)).all().get();

// Список и описание
Set<String> names = admin.listTopics().names().get();
Map<String, TopicDescription> desc = admin.describeTopics(List.of("orders")).allTopicNames().get();
desc.forEach((n, d) -> d.partitions().forEach(p ->
    System.out.printf("%s p%d leader=%s isr=%s%n",
        n, p.partition(), p.leader().id(), p.isr())));

// Конфигурация топика (retention и пр.)
Config config = admin.describeConfigs(
    List.of(new ConfigResource(ConfigResource.Type.TOPIC, "orders")))
    .all().get().get(new ConfigResource(ConfigResource.Type.TOPIC, "orders"));
config.entries().forEach(e -> System.out.println(e.name() + "=" + e.value()));

// Изменение конфигурации (retention 1 час)
admin.incrementalAlterConfigs(Map.of(
    new ConfigResource(ConfigResource.Type.TOPIC, "orders"),
    List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "3600000"),
        AlterConfigOp.OpType.SET))))
    .all().get();

// Удаление
admin.deleteTopics(List.of("orders")).all().get();
```

Идемпотентность: `createTopics(..., new CreateTopicsOptions().retryOnQuotaViolation(true))`
или проверка `listTopics().names()` перед созданием; `deleteTopics` с
`delete.topic.enable=true` (в нашем compose включено).

---

## 3. Управление партициями

Количество партиций топика можно **только увеличивать** (уменьшение не
поддерживается). Увеличение не перераспределяет существующие данные.

```java
admin.createPartitions(Map.of("orders", NewPartitions.increaseTo(6))).all().get();
```

Проверить:

```java
TopicDescription d = admin.describeTopics(List.of("orders"))
    .allTopicNames().get().get("orders");
System.out.println("partitions=" + d.partitions().size());
```

> Важно: ключ уже записанных сообщений перестаёт «попадать» в прежнюю партицию,
> т.к. hash считается по новому числу партиций. Для сохранения маршрутизации
> используют `CustomPartitioner` (Модуль 2).

---

## 4. Управление сообщениями и консюмерами

### Удаление сообщений (до offset)

```java
// Удалить все сообщения в партиции 0 до offset 10 (включительно = < offset 11)
admin.deleteRecords(Map.of(new TopicPartition("orders", 0), RecordsToDelete.beforeOffset(11L)))
    .all().get();
```

### Группы консюмеров

```java
// Список групп
admin.listConsumerGroups().all().get().forEach(g -> System.out.println(g.groupId()));

// Описание группы (состояние, участники, назначенные партиции)
admin.describeConsumerGroups(List.of("orders-group"))
    .all().get().forEach((id, g) -> {
        System.out.println("state=" + g.state());
        g.members().forEach(m -> System.out.println("  " + m.clientId() + " " + m.assignment()));
    });

// Офсеты группы по партициям (для расчёта лага)
Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets("orders-group")
    .partitionsToOffsetAndMetadata().get();
```

### Сброс офсетов (rewind)

```java
// Перемотать группу на начало топика orders.
// ВАЖНО: OffsetSpec — это не готовый offset, а «запрос»; его сначала резолвим
// через listOffsets(), затем передаём OffsetAndMetadata в alterConsumerGroupOffsets().
Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
specs.put(new TopicPartition("orders", 0), OffsetSpec.earliest());
specs.put(new TopicPartition("orders", 1), OffsetSpec.earliest());

Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
    admin.listOffsets(specs).all().get();

Map<TopicPartition, OffsetAndMetadata> reset = new HashMap<>();
for (TopicPartition tp : specs.keySet()) {
    reset.put(tp, new OffsetAndMetadata(earliest.get(tp).offset()));
}
admin.alterConsumerGroupOffsets("orders-group", reset).all().get();
```

Способы сброса: `OffsetSpec.earliest()`, `OffsetSpec.latest()`,
`OffsetSpec.forTimestamp(ts)`, `OffsetSpec.offset(offset)`.

> `alterConsumerGroupOffsets` падает с `UnknownMemberIdException`, если в группе
> ещё есть активные консюмеры. Перед сбросом остановите консюмеры группы (или
> дождитесь истечения `session.timeout.ms`).

---

## 5. Управление ACL и квотами

### ACL (Access Control Lists)

ACL управляют авторизацией: кто (`User:`/`Group:`) может выполнять операции
(`READ`, `WRITE`, `CREATE`, `DESCRIBE`, `DELETE`, `ALTER`) над ресурсами
(`TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`).

```java
// Разрешить пользователю alice читать топик orders
AclBinding binding = new AclBinding(
    new ResourcePattern(ResourceType.TOPIC, "orders", PatternType.LITERAL),
    new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));

admin.createAcls(List.of(binding)).all().get();
admin.describeAcls(AclBindingFilter.ANY).values().get().forEach(System.out::println);
admin.deleteAcls(List.of(AclBindingFilter.ANY)).all().get();
```

> **Важно:** чтобы ACL заработали, на брокере должен быть включён authorizer.
> В нашем compose по умолчанию его нет (`PLAINTEXT`, один разработчик), поэтому
> `createAcls` завершится ошибкой `No Authorizer is configured`. Для демонстрации
> добавьте в `docker-compose.yml` в блок `kafka.environment`:
>
> ```yaml
> KAFKA_AUTHORIZER_CLASS_NAME: kafka.security.authorizer.AclAuthorizer
> KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"
> KAFKA_SUPER_USERS: User:ANONYMOUS
> ```
>
> и пересоздайте брокера: `docker compose up -d --force-recreate kafka`.

### Квоты (quotas)

Квоты ограничивают производительность клиентов (защита от «шумного соседа»):

```java
// Лимит 1 МБ/с на producer/consumer для client-id "heavy-client"
admin.alterClientQuotas(List.of(new ClientQuotaAlteration(
    new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "heavy-client")),
    List.of(new ClientQuotaAlteration.Op(ClientQuotaConfig.PRODUCER_BYTE_RATE_QUOTA, 1024.0 * 1024.0)))))
    .all().get();

// Просмотр
admin.describeClientQuotas(ClientQuotaFilter.all()).entities().get()
    .forEach((e, q) -> System.out.println(e + " -> " + q));
```

Квоты применяются по `client.id`, `user` (с SASL) или `client.id + user`.

---

## Live coding: использование Admin API для управления кластером

Готовый fat-JAR — `dist/module04-admin/admin-example.jar`: запускаем без Maven, нужна только Java 17+. Исходники (для пересборки) — в `code/src/main/java/dev/kafka/AdminExample.java`.

### Шаг 1. Создание и описание топиков

```bash
java -jar dist/module04-admin/admin-example.jar create
java -jar dist/module04-admin/admin-example.jar describe
```

`create` создаёт `orders` (4 партиции) и `orders-agg` (4 партиции);
`describe` выводит лидера/ISR каждой партиции и конфигурацию.

### Шаг 2. Увеличение партиций

```bash
java -jar dist/module04-admin/admin-example.jar partitions
```

Проверяем: `orders` стал 6 партиций. Сравните в Kafdrop до/после.

### Шаг 3. Сообщения и консюмеры

```bash
# наполнить orders
java -jar dist/module02-producer/producer-example.jar
# запустить консюмер в фоне (в отдельном терминале)
java -jar dist/module03-consumer/consumer-example.jar

# посмотреть группы и офсеты
java -jar dist/module04-admin/admin-example.jar groups
```

### Шаг 4. Удаление сообщений до offset и сброс группы

```bash
java -jar dist/module04-admin/admin-example.jar delete-records
java -jar dist/module04-admin/admin-example.jar reset-offsets
```

После `reset-offsets` группа `orders-group` перечитает топик с начала.

### Шаг 5. Квоты

```bash
java -jar dist/module04-admin/admin-example.jar quota
java -jar dist/module04-admin/admin-example.jar list-quotas
```

### Шаг 6. ACL (после включения authorizer)

Добавьте authorizer в compose (см. §5), пересоздайте брокера и выполните:

```bash
java -jar dist/module04-admin/admin-example.jar acl
java -jar dist/module04-admin/admin-example.jar list-acls
```

### Шаг 7. Очистка

```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders-agg
```

---

## Итоги модуля

- `AdminClient` — программный доступ ко всем операциям администрирования.
- Топики: create/describe/delete + изменение конфигурации.
- Партиции: только увеличение (`createPartitions`).
- Сообщения: удаление до offset (`deleteRecords`); консюмеры: describe групп,
  офсеты, сброс (`alterConsumerGroupOffsets`).
- ACL требуют включённого authorizer на брокере; квоты работают сразу.

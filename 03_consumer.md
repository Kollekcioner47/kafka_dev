# Модуль 3. Kafka Consumer API

> 2 ак.ч · ~1 час теория + ~1 час live coding

## Цели модуля

- Понять группы консюмеров и стратегии распределения партиций.
- Разобраться, что такое rebalance и как его корректно обрабатывать.
- Освоить offset и стратегии коммита (auto / manual sync / manual async).
- Изучить ключевые параметры конфигурации консюмера.
- Создать `KafkaConsumer`, подписаться и читать сообщения.

---

## 1. Группы консюмеров и распределение партиций

Консюмеры объединяются в **consumer group** (параметр `group.id`). Правило:
**каждая партиция топика отдаётся не более чем одному консюмеру в группе**.
Один консюмер при этом может читать несколько партиций.

- консюмеров ≤ партиций → каждый получит ≥1 партицию, часть будет простаивать;
- консюмеров > партиций → «лишние» консюмеры останутся без партиций;
- разные группы читают топик независимо (каждая со своими офсетами).

### Стратегии распределения (`partition.assignment.strategy`)

| Стратегия | Поведение |
|---|---|
| `RangeAssignor` (дефолт) | партиции по диапазонам на топик; может дисбалансировать |
| `RoundRobinAssignor` | партиции по кругу, равномернее |
| `StickyAssignor` | минимум перемещений партиций при rebalance |
| `CooperativeStickyAssignor` | **кооперативный** rebalance: партиции освобождаются порциями, без «стоп-мира» |

Кооперативные стратегии (`CooperativeStickyAssignor`) — современный дефолт в
новых клиентах: при добавлении консюмера группа не останавливается целиком.

---

## 2. Rebalance

**Rebalance** — перераспределение партиций между живыми консюмерами группы.
Запускается при:
- добавлении/уходе консюмера из группы;
- изменении числа партиций топика;
- истечении `session.timeout.ms` (консюмер перестал слать heartbeat);
- превышении `max.poll.interval.ms` (консюмер не вызвал `poll()` вовремя).

### Обработка через ConsumerRebalanceListener

```java
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // Партиции забирают — успеть закоммитить офсеты ДО потери владения
        consumer.commitSync(currentOffsets());
        System.out.println("Revoked: " + partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Партиции назначили — можно seek в сохранённую позицию/начало
        System.out.println("Assigned: " + partitions);
    }
}
```

Подписка с listener:

```java
consumer.subscribe(List.of("orders"), new SaveOffsetsOnRebalance());
```

---

## 3. Offset

**Offset** — позиция чтения в партиции. Консюмер коммитит offset, чтобы после
рестарта продолжить с того же места.

### Три числа, которые объясняют состояние консюмера

Чтобы понять, «что происходит», следите за тремя числами по каждой партиции:

| Число | API | Смысл |
|---|---|---|
| **position** | `consumer.position(tp)` | до какого offset дочитали в этом сеансе (сдвигается при `poll`) |
| **committed** | `consumer.committed(tp)` | что сохранено для рестарта (сдвигается при `commitSync`/`commitAsync`) |
| **end / lag** | `consumer.endOffsets(tp)` | конец лога и сколько ещё не прочитано (`end − position`) |

Правило: `position` бежит вперёд при чтении, `committed` **догоняет** его при
коммите. Если они разошлись и процесс упал — после рестарта чтение продолжится
с `committed`, а «разрыв» (`position − committed`) будет прочитан **повторно**
(at-least-once). Если коммитить ДО обработки — наоборот, разрыв потеряется
(at-most-once).

В live coding используется `ConsumerVisualizer`, который после каждого `poll`
печатает эту таблицу — по ней видно, как числа ползут вперёд.

### Автокоммит (`enable.auto.commit=true`, дефолт)

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
```

Просто, но возможны дубли при падении между обработкой и автокоммитом
(at-least-once) и нет контроля над моментом фиксации.

### Ручной коммит (`enable.auto.commit=false`)

```java
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

// Синхронно — надёжно, но блокирует (рекомендуется на rebalance/выходе)
consumer.commitSync();

// Асинхронно — быстрее, callback для реакции на ошибку
consumer.commitAsync((offsets, e) -> {
    if (e != null) System.err.println("commit failed: " + e.getMessage());
});
```

Правило: коммитить **после успешной обработки** сообщения, иначе при сбое
сообщение потеряется (at-most-once).

### Сброс позиции (`auto.offset.reset`)

Работает, когда у группы ещё нет закоммиченного offset:

| Значение | Поведение |
|---|---|
| `earliest` | читать с самого начала топика |
| `latest` (дефолт) | читать только новые сообщения |
| `none` | ошибка, если offset не найден |

Ручное управление позицией:

```java
consumer.seek(new TopicPartition("orders", 0), 0L);          // в начало партиции
consumer.seekToBeginning(List.of(tp));                       // в начало
consumer.seekToEnd(List.of(tp));                             // в конец
```

---

## 4. Consumer API configuration

| Параметр | Дефолт | Назначение |
|---|---|---|
| `bootstrap.servers` | — | брокеры |
| `group.id` | — | имя группы (обязательно) |
| `key.deserializer` / `value.deserializer` | — | десериализаторы |
| `auto.offset.reset` | `latest` | откуда читать без офсета |
| `enable.auto.commit` | `true` | автокоммит |
| `auto.commit.interval.ms` | `5000` | период автокоммита |
| `max.poll.records` | `500` | сколько сообщений за один `poll()` |
| `max.poll.interval.ms` | `300000` | лимит между `poll()` до отчисления из группы |
| `session.timeout.ms` | `45000` | лимит heartbeat до признания «мёртвым» |
| `heartbeat.interval.ms` | `3000` | период heartbeat |
| `partition.assignment.strategy` | Range | стратегия распределения |
| `isolation.level` | `read_uncommitted` | видимость транзакционных сообщений (Модуль 5) |

---

## 5. Класс KafkaConsumer: создание, подписка, чтение

### Создание и подписка

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.GROUP_ID_CONFIG, "orders-group");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(List.of("orders"));          // подписка на топики
// либо явно:
// consumer.assign(List.of(new TopicPartition("orders", 0)));
```

`KafkaConsumer` **не потокобезопасен** — один экземпляр используется из одного
потока (на параллелизм — несколько консюмеров или отдельный поток).

### Цикл чтения

```java
try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> r : records) {
            System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                r.partition(), r.offset(), r.key(), r.value());
            // здесь — бизнес-обработка
        }
        consumer.commitSync();  // коммитим обработанные офсеты
    }
} finally {
    consumer.close();  // корректный выход из группы (без rebalance-timeout)
}
```

### Проверка позиции и лага

```java
consumer.assignment().forEach(tp -> {
    long pos = consumer.position(tp);
    long end = consumer.endOffsets(List.of(tp)).get(tp);
    System.out.printf("%s: pos=%d end=%d lag=%d%n", tp, pos, end, end - pos);
});
```

---

## Live coding: визуальная демонстрация консюмера

Исходный код — в `code/src/main/java/dev/kafka/ConsumerVisualizer.java`
(наглядный) и `ConsumerExample.java` (минимальный).

`ConsumerVisualizer` читает по 5 сообщений за `poll` (`max.poll.records=5`) и
после каждой порции печатает таблицу `partition | position | committed | end |
lag` — по ней видно, как числа ползут вперёд при чтении и коммите.

### Шаг 1. Данные

```bash
# 4 партиции
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --create --topic orders --partitions 4 --replication-factor 3
```

Наполняем топик 40 заказами через `ProducerExample` (Модуль 2):

```bash
cd code && mvn -q compile
mvn -q exec:java -Dexec.mainClass=dev.kafka.ProducerExample
```

### Шаг 2. Один консюмер — следим за тремя числами

```bash
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C1 earliest"
```

Разбираем вывод вместе:

1. Баннер `REBALANCE: мне НАЗНАЧЕНЫ партиции [orders-0, orders-1, orders-2, orders-3]` —
   один консюмер получил все 4 партиции.
2. Строки `poll вернул 5 сообщ.` и `partition=N offset=...` — что именно читается.
3. `commitSync()` — момент фиксации прочитанного.
4. Таблица: `position` и `committed` растут вместе (0 → 5 → 10 → … → 40), а
   `lag` уменьшается (40 → 35 → … → 0).

### Шаг 3. Два консюмера — распределение партиций и rebalance

Сбросим группу на начало, чтобы оба консюмера читали данные заново:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group orders-group --reset-offsets --to-earliest --topic orders --execute
```

Запустите **второй** консюмер (та же группа, другая метка):

```bash
# terminal 2
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C2 earliest"
```

Теперь каждый печатает **свой** набор партиций: например `C1 → [orders-0, orders-1]`,
`C2 → [orders-2, orders-3]`. В логе `C1` появится баннер
`REBALANCE: у меня ЗАБИРАЮТ партиции ...` — группа перераспределила партиции.

### Шаг 4. Уход консюмера — офсеты переживают rebalance

Остановите `C2` (Ctrl+C). `C1` покажет `REBALANCE: мне НАЗНАЧЕНЫ партиции ...`
и возьмёт все 4 партиции. **Важно:** он не перечитает то, что уже прочитал `C2` —
таблица покажет, что у «чужих» партиций `committed` уже на месте, а `lag = 0`.
Это и есть смысл коммита: прогресс группы сохраняется и не теряется при
перераспределении.

### Шаг 5. Offset reset: earliest vs latest

`auto.offset.reset` действует для **новой** группы (у которой ещё нет
закоммиченных офсетов), поэтому используем разные имена групп:

```bash
# свежая группа с earliest читает с начала
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C3 earliest g-earliest"

# свежая группа с latest не увидит старые 40 сообщений (только новые)
mvn -q exec:java -Dexec.mainClass=dev.kafka.ConsumerVisualizer -Dexec.args="C4 latest g-latest"
```

### Шаг 6. Очистка

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group orders-group --delete
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 --delete --topic orders
```

---

## Итоги модуля

- Партиция отдаётся ≤1 консюмеру группы; стратегии: Range/RoundRobin/Sticky/Cooperative.
- Rebalance запускается при изменении состава группы/партиций и обрабатывается
  через `ConsumerRebalanceListener`.
- Offset коммитится авто или вручную; `commitSync` надёжнее, `commitAsync` быстрее.
- `auto.offset.reset` управляет чтением новой группы (earliest/latest).
- `KafkaConsumer` не потокобезопасен; чтение — цикл `poll()` + обработка + коммит.
- Три числа `position`/`committed`/`end` показывают всё состояние консюмера.

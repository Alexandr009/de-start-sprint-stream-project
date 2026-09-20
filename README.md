# Проект спринта «Потоковая обработка данных»

Сервис уведомлений подписчиков ресторанов об акциях с ограниченным сроком
действия: Kafka → Spark Structured Streaming → Kafka + PostgreSQL.

## Как это работает

1. Ресторан публикует акцию — сообщение попадает во входной топик Kafka.
2. Приложение читает поток, разбирает JSON и оставляет только акции,
   действующие в момент обработки (`datetime_start <= now <= datetime_end`).
3. Действующие акции джойнятся со справочником подписчиков ресторанов
   из PostgreSQL по `restaurant_id` — каждая акция размножается на всех
   подписчиков ресторана.
4. Каждый микробатч уходит в два стока:
   - **Kafka** (выходной топик) — заготовки push-уведомлений в JSON;
   - **PostgreSQL** (локальная таблица `subscribers_feedback`) — те же строки
     с пустым полем `feedback` для последующего сбора обратной связи.

## Структура репозитория

```
src/
├── scripts/
│   └── restaurant_campaigns_stream.py   # стриминговое приложение
└── sql/
    └── subscribers_feedback.sql         # DDL выходной таблицы
```

## Источники и стоки

| Роль | Где | Параметры |
|---|---|---|
| Входной топик | Kafka `rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091` | `s19239739_in`, SASL_SSL / SCRAM-SHA-512 |
| Подписчики | PostgreSQL `rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de` | `public.subscribers_restaurants`, `student` |
| Выходной топик | Kafka, тот же кластер | `s19239739_out` |
| Фидбэк | PostgreSQL в Docker `localhost:5432/de` | `public.subscribers_feedback`, `jovyan` |

## Входное сообщение

Ключ произвольный, разделитель `:`, значение — JSON. Временные поля —
Unix time в секундах.

```
first_message:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru","adv_campaign_datetime_start": 1789908311,"adv_campaign_datetime_end": 1789915511,"datetime_created": 1789908311}
```

## Выходное сообщение

К полям акции добавляются `client_id` подписчика и `trigger_datetime_created` —
момент формирования уведомления.

```json
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003","adv_campaign_content":"first campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant.ru","adv_campaign_datetime_start":1789908311,"adv_campaign_datetime_end":1789915511,"datetime_created":1789908311,"client_id":"823e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1789911928}
```

В таблицу `subscribers_feedback` пишутся те же поля плюс `feedback` (`NULL`),
в Kafka поле `feedback` не отправляется.

## Запуск

Порядок действий:

1. Создать выходную таблицу в локальном PostgreSQL:

```bash
psql -h localhost -U jovyan -d de -f src/sql/subscribers_feedback.sql
```

2. Запустить приложение (зависимости Kafka и PostgreSQL подтягиваются
   из Maven через `spark.jars.packages`):

```bash
python3 src/scripts/restaurant_campaigns_stream.py
```

   Для отладки шагов чтения, разбора JSON, фильтра и join результат можно
   выводить в консоль вместо стоков:

```bash
DEBUG=1 python3 src/scripts/restaurant_campaigns_stream.py
```

   Имена топиков и учётные данные берутся из переменных окружения
   (`TOPIC_IN`, `TOPIC_OUT`, `KAFKA_USER`, `KAFKA_PASSWORD`, `SRC_PG_USER`,
   `SRC_PG_PASSWORD`, `DST_PG_URL`, `DST_PG_USER`, `DST_PG_PASSWORD`);
   значения по умолчанию соответствуют учебному окружению.

3. Отправить тестовую акцию во входной топик — время действия должно
   включать текущий момент:

```bash
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt -t s19239739_in -K: -P
```

4. Проверить выходной топик и таблицу:

```bash
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt -t s19239739_out -C -o beginning -e
```

```bash
psql -h localhost -U jovyan -d de -c "select * from public.subscribers_feedback order by id"
```

## Принятые решения

**Фильтр по времени действия** сравнивает `adv_campaign_datetime_start` и
`adv_campaign_datetime_end` с `unix_timestamp(current_timestamp())` в момент
обработки микробатча; сессия Spark работает в UTC, чтобы Unix time из
сообщений и текущее время были в одной шкале. Просроченная или ещё не
начавшаяся акция в стоки не попадает.

**Join — stream-static inner** по `restaurant_id`. Справочник подписчиков
небольшой и рассылается бродкастом (`f.broadcast`), поэтому поток не
шаффлится. Дубли в справочнике убираются `distinct()` до join, чтобы один
подписчик не получил уведомление дважды.

**Два стока через `foreachBatch`.** Микробатч кэшируется (`persist`),
записывается в PostgreSQL и Kafka, затем кэш освобождается (`unpersist`) —
join не пересчитывается для второго стока.

**`trigger_datetime_created`** вычисляется как `current_timestamp()` при
обработке батча, а не при построении плана, поэтому у каждого микробатча
своё время.

**`spark.sql.shuffle.partitions = 4`.** Значение по умолчанию (200) на
потоке из единиц сообщений только замедляет каждый микробатч.

**Checkpoint** хранится в `/tmp/checkpoints/restaurant_campaigns` — после
перезапуска приложение продолжает с последнего обработанного офсета и не
отправляет уведомления повторно.

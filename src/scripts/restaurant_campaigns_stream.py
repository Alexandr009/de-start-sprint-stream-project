"""Стриминг акций ресторанов подписчикам.

Читает акции из Kafka, отбирает действующие сейчас, джойнит с подписчиками
ресторана из Postgres и раскладывает результат в два стока: Kafka для сервиса
push-уведомлений и локальный Postgres для сбора фидбэка.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import LongType, StringType, StructField, StructType

TOPIC_IN = os.environ.get('TOPIC_IN', 's19239739_in')
TOPIC_OUT = os.environ.get('TOPIC_OUT', 's19239739_out')

KAFKA_BOOTSTRAP = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'
KAFKA_USER = os.environ.get('KAFKA_USER', 'de-student')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD', 'ltcneltyn')

# DEBUG=1 — результат join выводится в консоль вместо записи в стоки:
# так проверяются чтение, разбор JSON, фильтр и join по отдельности
DEBUG = os.environ.get('DEBUG', '0') == '1'

# библиотеки для Kafka и Postgres из Maven
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

kafka_security_options = {
    'kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': (
        'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
    ),
}

# входная таблица подписчиков — в облачном Postgres
subscribers_postgres_options = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_restaurants',
    'user': os.environ.get('SRC_PG_USER', 'student'),
    'password': os.environ.get('SRC_PG_PASSWORD', 'de-student'),
}

# выходная таблица фидбэка — в локальном Postgres из Docker
feedback_postgres_options = {
    'url': os.environ.get('DST_PG_URL', 'jdbc:postgresql://localhost:5432/de'),
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
    'user': os.environ.get('DST_PG_USER', 'jovyan'),
    'password': os.environ.get('DST_PG_PASSWORD', 'jovyan'),
}

# схема входного сообщения об акции; временные поля — Unix time в секундах
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

OUTPUT_COLUMNS = [
    "restaurant_id",
    "adv_campaign_id",
    "adv_campaign_content",
    "adv_campaign_owner",
    "adv_campaign_owner_contact",
    "adv_campaign_datetime_start",
    "adv_campaign_datetime_end",
    "datetime_created",
    "client_id",
    "trigger_datetime_created",
]


def spark_init() -> SparkSession:
    # поток небольшой: 200 шаффл-партиций по умолчанию только замедляют
    # каждый микробатч
    return SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()


def read_campaigns_stream(spark: SparkSession) -> DataFrame:
    """Акции из Kafka; оставляем только действующие в момент обработки."""
    raw = spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('subscribe', TOPIC_IN) \
        .load()

    campaigns = raw \
        .withColumn("value", f.col("value").cast("string")) \
        .withColumn("parsed", f.from_json(f.col("value"), incomming_message_schema)) \
        .select("parsed.*")

    now = f.unix_timestamp(f.current_timestamp())

    return campaigns.where(
        (f.col("adv_campaign_datetime_start") <= now)
        & (f.col("adv_campaign_datetime_end") >= now)
    )


def read_subscribers(spark: SparkSession) -> DataFrame:
    """Кто на какие рестораны подписан — статичный справочник."""
    return spark.read \
        .format('jdbc') \
        .options(**subscribers_postgres_options) \
        .load() \
        .select("client_id", "restaurant_id") \
        .distinct()


def join_campaigns_with_subscribers(campaigns: DataFrame, subscribers: DataFrame) -> DataFrame:
    """Каждая действующая акция — каждому подписчику её ресторана.

    Справочник подписчиков маленький, поэтому рассылается бродкастом —
    stream-static join обходится без шаффла потока.
    """
    return campaigns \
        .join(f.broadcast(subscribers), "restaurant_id", "inner") \
        .withColumn("trigger_datetime_created", f.unix_timestamp(f.current_timestamp())) \
        .select(*OUTPUT_COLUMNS)


def write_feedback(df: DataFrame) -> None:
    """Заготовка под фидбэк: те же поля плюс пустой feedback."""
    df.withColumn("feedback", f.lit(None).cast(StringType())) \
        .write \
        .format('jdbc') \
        .mode('append') \
        .options(**feedback_postgres_options) \
        .save()


def write_push_notifications(df: DataFrame) -> None:
    """Сериализуем строку в JSON и кладём в value — Kafka принимает только его."""
    df.select(f.to_json(f.struct(*OUTPUT_COLUMNS)).alias("value")) \
        .write \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('topic', TOPIC_OUT) \
        .save()


def foreach_batch_function(df: DataFrame, epoch_id: int) -> None:
    # микробатч уходит в два стока — кэшируем, чтобы не пересчитывать join дважды
    df.persist()

    write_feedback(df)
    write_push_notifications(df)

    df.unpersist()


def main():
    spark = spark_init()

    campaigns = read_campaigns_stream(spark)
    subscribers = read_subscribers(spark)
    result = join_campaigns_with_subscribers(campaigns, subscribers)

    if DEBUG:
        subscribers.show(truncate=False)
        writer = result.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False)
    else:
        writer = result.writeStream \
            .foreachBatch(foreach_batch_function) \
            .option("checkpointLocation", "/tmp/checkpoints/restaurant_campaigns")

    query = writer.trigger(processingTime="30 seconds").start()
    query.awaitTermination()


if __name__ == "__main__":
    main()

import os
import json
import psycopg2
from psycopg2.extras import execute_batch
from confluent_kafka import Consumer
import logging
from collections import defaultdict

# logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("kafka-batch-consumer")

BATCH_SIZE = 5000

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SECRET_PATH = "/etc/kafka/secrets"
KAFKA_CA_PATH = "/etc/kafka/ca"
DB_HOST = os.getenv("PGHOST")
DB_PORT = os.getenv("PGPORT", 5432)
DB_NAME = os.getenv("PGDATABASE")
DB_USER = os.getenv("PGUSER")
DB_PASSWORD = os.getenv("PGPASSWORD")

if not all(
    [
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        DB_HOST,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
    ]
):
    raise EnvironmentError(
        "Missing required Kafka or DB environment variables"
    )


def read_secret(path):
    with open(path, "r") as f:
        return f.read().strip()


KAFKA_KEY_PASSWORD = read_secret(
    os.path.join(KAFKA_SECRET_PATH, "user.password")
)
KAFKA_KEY = os.path.join(KAFKA_SECRET_PATH, "user.key")
KAFKA_CERT = os.path.join(KAFKA_SECRET_PATH, "user.crt")
KAFKA_CA = os.path.join(KAFKA_CA_PATH, "ca.crt")

consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SSL",
    "ssl.key.location": KAFKA_KEY,
    "ssl.certificate.location": KAFKA_CERT,
    "ssl.ca.location": KAFKA_CA,
    "ssl.key.password": KAFKA_KEY_PASSWORD,
    "group.id": "finure",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

db_conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)
db_conn.autocommit = False

insert_sql = """
INSERT INTO application_record (
    id, code_gender, flag_own_car, flag_own_realty, cnt_children,
    amt_income_total, name_income_type, name_education_type, name_family_status,
    name_housing_type, days_birth, days_employed, flag_mobil, flag_work_phone,
    flag_phone, flag_email, occupation_type, cnt_fam_members
)
VALUES (
    %(id)s, %(code_gender)s, %(flag_own_car)s, %(flag_own_realty)s, %(cnt_children)s,
    %(amt_income_total)s, %(name_income_type)s, %(name_education_type)s, %(name_family_status)s,
    %(name_housing_type)s, %(days_birth)s, %(days_employed)s, %(flag_mobil)s, %(flag_work_phone)s,
    %(flag_phone)s, %(flag_email)s, %(occupation_type)s, %(cnt_fam_members)s
)
ON CONFLICT (id) DO NOTHING
"""


def process_batch(messages):
    partition_offsets = defaultdict(list)
    records = []
    for m in messages:
        try:
            data = json.loads(m.value())
            record = {
                "id": data["id"],
                "code_gender": data.get("code_gender"),
                "flag_own_car": data.get("flag_own_car"),
                "flag_own_realty": data.get("flag_own_realty"),
                "cnt_children": data.get("cnt_children"),
                "amt_income_total": data.get("amt_income_total"),
                "name_income_type": data.get("name_income_type"),
                "name_education_type": data.get("name_education_type"),
                "name_family_status": data.get("name_family_status"),
                "name_housing_type": data.get("name_housing_type"),
                "days_birth": data.get("days_birth"),
                "days_employed": data.get("days_employed"),
                "flag_mobil": data.get("flag_mobil"),
                "flag_work_phone": data.get("flag_work_phone"),
                "flag_phone": data.get("flag_phone"),
                "flag_email": data.get("flag_email"),
                "occupation_type": data.get("occupation_type"),
                "cnt_fam_members": data.get("cnt_fam_members"),
                "approved": data.get("approved"),
                "approval_date": data.get("approval_date"),
                "risk_score": data.get("risk_score"),
                "risky": data.get("risky"),
                "external_data_last_checked": data.get(
                    "external_data_last_checked"
                ),
            }
            records.append(record)
            partition_offsets[m.partition()].append(m.offset())
        except Exception as e:
            logger.warning(f"Skipping bad message: {e}")

    if not records:
        logger.info("No valid records to insert in this batch")
        return

    with db_conn.cursor() as cur:
        try:
            execute_batch(cur, insert_sql, records, page_size=BATCH_SIZE)
            db_conn.commit()
            logger.info(f"Inserted {len(records)} records into Postgres")
        except Exception as e:
            db_conn.rollback()
            logger.error(f"DB insert error: {e}")
            raise

    for partition, offsets in partition_offsets.items():
        if offsets:
            logger.info(
                f"Partition {partition}: offsets {min(offsets)} to {max(offsets)} (count={len(offsets)})"
            )


def main():
    c = Consumer(consumer_conf)

    def on_assign(consumer, partitions):
        logger.info(
            "Partitions assigned to this consumer: %s",
            [f"{p.topic}-{p.partition}" for p in partitions],
        )

    c.subscribe([KAFKA_TOPIC], on_assign=on_assign)

    batch = []
    total_processed = 0
    try:
        while True:
            msgs = c.consume(num_messages=BATCH_SIZE, timeout=5.0)
            if msgs:
                good_msgs = [m for m in msgs if m and not m.error()]
                batch.extend(good_msgs)
            while len(batch) >= BATCH_SIZE:
                to_process = batch[:BATCH_SIZE]
                process_batch(to_process)
                total_processed += len(to_process)
                c.commit()
                logger.info(
                    f"Committed offsets after batch of {len(to_process)} messages"
                )
                batch = batch[BATCH_SIZE:]
            if not msgs and batch:
                process_batch(batch)
                total_processed += len(batch)
                c.commit()
                logger.info(
                    f"Committed offsets after batch of {len(batch)} messages"
                )
                batch = []
            if not msgs:
                logger.info("No new messages polled")
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully")
    finally:
        if batch:
            process_batch(batch)
            total_processed += len(batch)
            c.commit()
            logger.info(
                f"Committed offsets after final batch of {len(batch)} messages"
            )
        logger.info(f"Total messages processed: {total_processed}")
        c.close()
        db_conn.close()


if __name__ == "__main__":
    main()

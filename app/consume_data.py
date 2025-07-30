import os
import json
import time
import signal
from queue import Queue, Empty
from threading import Thread
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

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
    "session.timeout.ms": 90000,
    "max.poll.interval.ms": 1800000,
    "heartbeat.interval.ms": 30000,
    "request.timeout.ms": 120000,
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

record_queue = Queue()
commit_queue = Queue()
running = True
processed = 0
committed = 0
BATCH_SIZE = 2000
COMMIT_INTERVAL = 5


def shutdown(sig, frame):
    global running
    print("\nShutdown signal received")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def db_worker():
    global committed
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = False
    cur = conn.cursor()
    batch = []
    last_commit_time = time.time()

    while running or not record_queue.empty():
        try:
            record = record_queue.get(timeout=1)
            batch.append(record)
        except Empty:
            pass

        if len(batch) >= BATCH_SIZE or (
            time.time() - last_commit_time > COMMIT_INTERVAL and batch
        ):
            try:
                execute_values(
                    cur,
                    """
                    INSERT INTO application_record (
                        id, code_gender, flag_own_car, flag_own_realty, cnt_children,
                        amt_income_total, name_income_type, name_education_type,
                        name_family_status, name_housing_type, days_birth,
                        days_employed, flag_mobil, flag_work_phone, flag_phone,
                        flag_email, occupation_type, cnt_fam_members
                    ) VALUES %s
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    [
                        (
                            r["id"],
                            r["code_gender"],
                            r["flag_own_car"],
                            r["flag_own_realty"],
                            r["cnt_children"],
                            r["amt_income_total"],
                            r["name_income_type"],
                            r["name_education_type"],
                            r["name_family_status"],
                            r["name_housing_type"],
                            r["days_birth"],
                            r["days_employed"],
                            r["flag_mobil"],
                            r["flag_work_phone"],
                            r["flag_phone"],
                            r["flag_email"],
                            r.get("occupation_type"),
                            r["cnt_fam_members"],
                        )
                        for r in batch
                    ],
                )
                conn.commit()
                committed += len(batch)
                print(f"Committed {committed} records to DB")
                batch.clear()
                last_commit_time = time.time()
            except Exception as e:
                print(f"Batch insert failed: {e}")
                conn.rollback()

    cur.close()
    conn.close()


def commit_worker():
    last_commit_time = time.time()
    pending_messages = []

    while running or not commit_queue.empty():
        try:
            msg = commit_queue.get(timeout=2)
            pending_messages.append(msg)
        except Empty:
            pass

        should_commit = len(pending_messages) >= 30000 or (
            time.time() - last_commit_time > 60 and pending_messages
        )

        if should_commit:
            try:
                consumer.commit(pending_messages[-1], asynchronous=False)
                print(f"Kafka: Committed {len(pending_messages)} offsets")
                pending_messages.clear()
                last_commit_time = time.time()
            except Exception as e:
                print(f"Kafka commit failed: {e}")
                if len(pending_messages) > 50000:
                    pending_messages = pending_messages[-25000:]


db_thread = Thread(target=db_worker)
db_thread.start()

commit_thread = Thread(target=commit_worker)
commit_thread.start()


def handle_message(msg):
    global processed
    try:
        record = json.loads(msg.value().decode("utf-8"))
        record_queue.put(record)
        commit_queue.put(msg)
        processed += 1
        if processed % 5000 == 0:
            print(f"Processed {processed} messages from Kafka")
    except Exception as e:
        print(f"Failed to process message: {e}")
        print(f"Payload: {msg.value()}")


print("Consuming messages from Kafka")
try:
    while running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka error: {msg.error()}")
                continue
        handle_message(msg)

finally:
    print("\nShutting down")
    consumer.close()
    db_thread.join()
    commit_thread.join()
    print(f"Final total processed: {processed}, committed to DB: {committed}")

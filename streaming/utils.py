import logging
import socket

from confluent_kafka import Producer, Consumer

from settings import KAFKA_BROKER


def create_producer():
    try:
        producer = Producer({"bootstrap.servers": KAFKA_BROKER,
                             "client.id": socket.gethostname(),
                             "enable.idempotence": True,  # EOS processing
                             "acks": "all",  # Wait for leader and all ISR to send response back
                             "retries": 5,
                             "delivery.timeout.ms": 1000})  # Total time to make retries
    except Exception as e:
        logging.exception("Couldn't create the producer")
        producer = None
    return producer


def create_consumer(topic, group_id):
    try:
        consumer = Consumer({"bootstrap.servers": KAFKA_BROKER,
                             "group.id": group_id,
                             "auto.offset.reset": "latest",
                             "client.id": socket.gethostname(),
                             "isolation.level": "read_committed",
                             "enable.auto.commit": False
                             })

        consumer.subscribe([topic])
    except Exception as e:
        logging.exception("Couldn't create the producer")
        consumer = None

    return consumer

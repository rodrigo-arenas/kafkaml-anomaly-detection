import logging
import json
import socket

import numpy as np
from confluent_kafka import Producer

from settings import KAFKA_BROKER, PRODUCER_TOPIC

rng = np.random.RandomState(42)

try:
    producer = Producer({"bootstrap.servers": KAFKA_BROKER,
                         "client.id": socket.gethostname(),
                         "enable.idempotence": True,  # EOS processing
                         "acks": "all",  # Wait for leader and all ISR to send response back
                         "retries": 5,
                         "delivery.timeout.ms": 60000,  # Total time to make retries
                         "compression.type": "lz4",  # Compression strategy
                         "batch.size": 64000,  # Bytes of messages to send in batch
                         "linger.ms": 200})  # Time to wait until fill the batch.size
except Exception as e:
    logging.exception("Couldn't create the producer")
    producer = None

_id = 0
if producer is not None:
    while True:
        # Generate some abnormal novel observations
        X_outliers = rng.uniform(low=-4, high=4, size=(1, 2)).tolist()

        record = {"id": _id, "data": X_outliers}
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=PRODUCER_TOPIC,
                         value=record,
                         key=str(_id))
        producer.flush()
        _id += 1

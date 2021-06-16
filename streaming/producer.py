import logging
import json
import socket
import time

import numpy as np
from confluent_kafka import Producer

from settings import KAFKA_BROKER, PRODUCER_TOPIC


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


if __name__ == "__main__":

    rng = np.random.RandomState(42)
    _id = 0
    producer = create_producer()

    if producer is not None:
        while True:
            # Generate some abnormal novel observations
            X_outliers = rng.uniform(low=-3, high=3, size=(1, 2)).tolist()

            record = {"id": _id, "data": X_outliers}
            record = json.dumps(record).encode("utf-8")

            producer.produce(topic=PRODUCER_TOPIC,
                             value=record,
                             key=str(_id))
            producer.flush()
            _id += 1
            time.sleep(2)

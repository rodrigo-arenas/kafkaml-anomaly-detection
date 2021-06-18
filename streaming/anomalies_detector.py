import json
import os
from joblib import load
import logging
from multiprocessing import Process

import numpy as np

from streaming.utils import create_producer, create_consumer
from settings import TRANSACTIONS_TOPIC, TRANSACTIONS_CONSUMER_GROUP, ANOMALIES_TOPIC, NUM_PARTITIONS

model_path = os.path.abspath('../model/isolation_forest.joblib')


def detect():
    consumer = create_consumer(topic=TRANSACTIONS_TOPIC, group_id=TRANSACTIONS_CONSUMER_GROUP)

    producer = create_producer()

    clf = load(model_path)

    while True:
        message = consumer.poll(timeout=50)
        if message is None:
            continue
        if message.error():
            logging.error("Consumer error: {}".format(message.error()))
            continue

        # Message that came from producer
        record = json.loads(message.value().decode('utf-8'))
        data = record["data"]

        prediction = clf.predict(data)

        # If an anomaly comes in, send it to anomalies topic
        if prediction[0] == -1:
            score = clf.score_samples(data)
            record["score"] = np.round(score, 3).tolist()

            _id = str(record["id"])
            record = json.dumps(record).encode("utf-8")

            producer.produce(topic=ANOMALIES_TOPIC,
                             value=record)
            producer.flush()

        # consumer.commit() # Uncomment to process all messages, not just new ones

    consumer.close()


# One consumer per partition
for _ in range(NUM_PARTITIONS):
    p = Process(target=detect)
    p.start()

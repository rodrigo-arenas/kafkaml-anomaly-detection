import json
import os
from joblib import load
import logging

from streaming.utils import create_producer, create_consumer
from settings import TRANSACTIONS_TOPIC, TRANSACTIONS_CONSUMER_GROUP, ANOMALIES_TOPIC

model_path = os.path.abspath('../model/isolation_forest.joblib')

clf = load(model_path)

consumer = create_consumer(topic=TRANSACTIONS_TOPIC, group_id=TRANSACTIONS_CONSUMER_GROUP)

producer = create_producer()

while True:
    message = consumer.poll()
    if message is None:
        continue
    if message.error():
        logging.error("Consumer error: {}".format(message.error()))
        continue

    # Message that came from producer
    record = json.loads(message.value().decode('utf-8'))
    data = record["data"]

    prediction = clf.predict(data)
    score = clf.score_samples(data)
    record["score"] = score.tolist()

    # If an anomaly comes in, send it to anomalies topic
    if prediction[0] == -1:

        _id = str(record["id"])
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=ANOMALIES_TOPIC,
                         value=record,
                         key=_id)
        producer.flush()

    consumer.commit()

consumer.close()

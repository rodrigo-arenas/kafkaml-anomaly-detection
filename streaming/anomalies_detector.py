import json
import os
from joblib import load
import socket
import logging

from confluent_kafka import Consumer

from streaming.producer import create_producer
from settings import KAFKA_BROKER, ANOMALIES_TOPIC


model_path = os.path.abspath('../model/isolation_forest.joblib')

clf = load(model_path)

consumer = Consumer({"bootstrap.servers": KAFKA_BROKER,
                     "group.id": "transactions_reader",
                     "auto.offset.reset": "earliest",
                     "client.id": socket.gethostname(),
                     "isolation.level": "read_committed",
                     "enable.auto.commit": False
                     })

consumer.subscribe(["transactions"])

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

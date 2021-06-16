import json
import time

import numpy as np

from settings import TRANSACTIONS_TOPIC
from streaming.utils import create_producer

_id = 0
producer = create_producer()

if producer is not None:
    while True:
        # Generate some abnormal observations
        X = 0.3 * np.random.rand(1, 2)
        X_test = (X + np.random.uniform(low=-1.5, high=1.5)).tolist()

        record = {"id": _id, "data": X_test}
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=TRANSACTIONS_TOPIC,
                         value=record,
                         key=str(_id))
        producer.flush()
        _id += 1
        time.sleep(1)

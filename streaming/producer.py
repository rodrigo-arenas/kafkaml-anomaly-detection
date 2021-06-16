import json
import time

import numpy as np

from settings import TRANSACTIONS_TOPIC
from streaming.utils import create_producer

rng = np.random.RandomState(42)
_id = 0
producer = create_producer()

if producer is not None:
    while True:
        # Generate some abnormal novel observations
        X_outliers = rng.uniform(low=-2, high=2, size=(1, 2)).tolist()

        record = {"id": _id, "data": X_outliers}
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=TRANSACTIONS_TOPIC,
                         value=record,
                         key=str(_id))
        producer.flush()
        _id += 1
        time.sleep(1)

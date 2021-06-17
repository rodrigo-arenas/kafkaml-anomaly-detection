import json
import time
from datetime import datetime

import numpy as np

from settings import TRANSACTIONS_TOPIC, DELAY
from streaming.utils import create_producer

_id = 0
producer = create_producer()

if producer is not None:
    while True:
        # Generate some abnormal observations
        X = 0.3 * np.random.randn(1, 2)
        X_test = (X + np.random.choice(a=[3, 2, -2], size=1, p=[0.05, 0.45, 0.5]))
        X_test = np.round(X_test, 3).tolist()

        current_time = datetime.utcnow().isoformat()

        record = {"id": _id, "data": X_test, "current_time": current_time}
        record = json.dumps(record).encode("utf-8")

        producer.produce(topic=TRANSACTIONS_TOPIC,
                         value=record,
                         key=str(_id))
        producer.flush()
        _id += 1
        time.sleep(DELAY)

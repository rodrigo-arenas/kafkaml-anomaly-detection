import json
import logging

from slack import WebClient
from slack.errors import SlackApiError

from settings import SLACK_API_TOKEN, SLACK_CHANNEL, ANOMALIES_TOPIC, ANOMALIES_CONSUMER_GROUP
from streaming.utils import create_consumer

client = WebClient(token=SLACK_API_TOKEN)

consumer = create_consumer(topic=ANOMALIES_TOPIC, group_id=ANOMALIES_CONSUMER_GROUP)

while True:
    message = consumer.poll()
    if message is None:
        continue
    if message.error():
        logging.error("Consumer error: {}".format(message.error()))
        continue

    # Message that came from producer
    record = message.value().decode('utf-8')

    try:
        # Send message to slack channel
        response = client.chat_postMessage(channel=SLACK_CHANNEL,
                                           text=record)
    except SlackApiError as e:

        print(e.response["error"])

    consumer.commit()

consumer.close()

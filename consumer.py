import threading
import logging
import time
import argparse
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from datetime import datetime


class Consumer(threading.Thread):
    daemon = False

    def __init__(self, topic, bootstrap_server, group):
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        self.group = group
        threading.Thread.__init__(self)

    def run(self):
        client = KafkaClient(self.bootstrap_server, client_id='commandline')
        consumer = SimpleConsumer(client, self.group, self.topic, auto_commit_every_n=1, buffer_size=160,
                                  auto_commit=True)

        for message in consumer:
            now = datetime.now()
            print("%s: %s" % (now, message))
            consumer.commit()


def main(kafka_server, topic, group):
    threads = [
        Consumer(topic, kafka_server, group)
    ]

    for t in threads:
        t.start()
    time.sleep(5)


if __name__ == "__main__":
    # Initially based on https://github.com/mumrah/kafka-python/blob/master/example.py
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARN
    )
    parser = argparse.ArgumentParser(description='Apache Kafka consumer. Can be used as a topic monitor')
    parser.add_argument('--kafka', help='Comma-separated kafka brokers', required=True)
    parser.add_argument('--topic', help='Topic name', required=True)
    parser.add_argument('--group', help='Consumer group', default='kafka-python')

    args = parser.parse_args()

    main(args.kafka, args.topic, args.group)

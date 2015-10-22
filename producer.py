import threading
import logging
import time
import argparse
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from datetime import datetime


class Producer(threading.Thread):
    daemon = False

    def __init__(self, topic, bootstrap_server):
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        threading.Thread.__init__(self)

    def run(self):
        client = KafkaClient(self.bootstrap_server)
        producer = SimpleProducer(client)
        i = 0
        while True:
            d = datetime.now()
            producer.send_messages(self.topic, ("%s:%s" % (i, d)).encode('utf-8'))
            i += 1
            time.sleep(2)


def main(kafka_server, topic):
    threads = [
        Producer(topic, kafka_server)
    ]

    for t in threads:
        t.start()


if __name__ == "__main__":
    # Initially based on https://github.com/mumrah/kafka-python/blob/master/example.py
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARN
    )
    parser = argparse.ArgumentParser(description='Apache Kafka producer. Can be used as a topic monitor')
    parser.add_argument('--kafka', help='Comma-separated kafka brokers', required=True)
    parser.add_argument('--topic', help='Topic name', required=True)

    args = parser.parse_args()

    main(args.kafka, args.topic)

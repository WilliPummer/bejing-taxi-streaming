import logging
import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, KafkaError


class KafkaListener:

    def __init__(self, hosts, group, topic):
        self.conf = {'bootstrap.servers': hosts, 'group.id': group,
                     'auto.offset.reset': 'latest'}
        self.topic = topic
        self.group = group
        self.consumer = None

    def __del__(self):
        self.close_consumer()

    def close_consumer(self):
        logging.info(f'Stop streaming with group {self.group}')
        try:
            if not self.consumer is None:
                self.consumer.close()
        except:
            logging.error("Error on closing")

    def consume(self):
        try:
            self.consumer = Consumer(self.conf)
            self.consumer.subscribe(self.topic)
            logging.info(f'Start to consume messages with group {self.group}')
            last_msg = datetime.now()
            while True:
                msg = self.consumer.poll(timeout=1.0)
                logging.debug('Polling...')
                if msg is None:
                    cur_time = datetime.now()
                    if (cur_time - last_msg).total_seconds() > 5:
                        last_msg = datetime.now()
                        yield "EMPTY_MSG"
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logging.error('%% %s [%d] reached end at offset %d\n' %
                                      (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    logging.debug('Got Message...')
                    last_msg = datetime.now()
                    yield json.loads(msg.value())
        finally:
            self.close_consumer()

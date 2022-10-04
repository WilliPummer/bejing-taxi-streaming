import logging
import json

from confluent_kafka import Consumer, KafkaException, KafkaError


class KafkaListener:

    def __init__(self, hosts, group, topic):
        self.conf = {'bootstrap.servers': hosts, 'group.id': group,
                     'auto.offset.reset': 'smallest'}
        self.topic = topic
        self.group = group

    def consume(self):
        try:
            consumer = Consumer(self.conf)
            consumer.subscribe(self.topic)
            logging.info(f'Start to consume messages with group {self.group}')
            while True:
                msg = consumer.poll(timeout=1.0)
                logging.debug('Polling...')
                if msg is None:
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
                    yield json.loads(msg.value())
        finally:
            # Close down consumer to commit final offsets.
            consumer.close()

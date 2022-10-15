import logging
import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, KafkaError


class KafkaListener:
    """
    Class to manage a Kafka consumer. Provides message as generator, that start with the latest offset
    of the given topic.

    Attributes
    ----------
    hosts : str
        Kafka connection string
    group : str
        consumer group name
    topic : str
        topic to read from

    Methods
    -------
    close_consumer()
        Closes the internal Kafka consumer. This method is also called on object deletion

    consume()
        Starts message consumption as generator, that yields the next messages as json. Yields the
        message 'EMPTY_MSG' every 5 sec if no message is available to prevent the stream from closing.

    """

    def __init__(self, hosts, group, topic):
        self.conf = {'bootstrap.servers': hosts, 'group.id': group,
                     'auto.offset.reset': 'latest'}
        self.topic = topic
        self.group = group
        self.consumer = None
        self.last_msg = None

    def __del__(self):
        self.close_consumer()

    def close_consumer(self):
        """
        Closes the internal Kafka consumer. This method is also called on object deletion
        """
        logging.info(f'Stop streaming with group {self.group}')
        try:
            if self.consumer is not None:
                self.consumer.close()
        except:
            logging.error("Error on closing")

    def consume(self):
        """
        Starts message consumption as generator, that yields the next messages as json. Yields the
        message 'EMPTY_MSG' every 5 sec if no message is available to prevent the stream from closing.
        """
        try:
            self.consumer = Consumer(self.conf)
            self.consumer.subscribe(self.topic)
            logging.info(f'Start to consume messages with group {self.group}')
            self.last_msg = datetime.now()
            while True:
                msg = self.consumer.poll(timeout=1.0)
                logging.debug('Polling...')
                if msg is None:
                    if _ping_required():
                        yield "EMPTY_MSG"
                    continue
                elif msg.error():
                    _handle_error(msg)
                else:
                    logging.debug('Got Message...')
                    self.last_msg = datetime.now()
                    yield json.loads(msg.value())
        finally:
            self.close_consumer()

    def _ping_required(self):
        # Checks if ping is needed (5 sec after last message).
        cur_time = datetime.now()
        if (cur_time - last_msg).total_seconds() > 5:
            self.last_msg = cur_time
            return True
        return False


def _handle_error(msg):
    # Handles error from consumer
    if msg.error().code() == KafkaError._PARTITION_EOF:
        # End of partition event
        logging.error('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
    elif msg.error():
        raise KafkaException(msg.error())

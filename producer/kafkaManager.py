from ctypes import c_bool
from multiprocessing import Manager, Value, Process
import datetime
import time
import dateutil
import pandas as pd
import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import socket


def singleton(class_):
    """
    decorator to ensure singleton classes
    """
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class KafkaManager:
    """
    Class to manage a Kafka producer as a singelton. It streams a source file as source file in
    pseudo 'real-time'. The  source file is streamed relative to the time at the first call to start().
    The production loop runs in a Process that is controllable with this class.

    Attributes
    ----------
    hosts : str
        Kafka connection string
    topic : str
        target topic for Kafka
    file : str
        source file as path, requires a date column

    Methods
    -------
    index()
        Returns the last processed index (row number)

    toggle_silenced()
        Suppresses the delivery of the messages to Kafka and returns a status
        ('Not sending Messages','Sending again messages')

    start()
        Stars the delivery process and returns a status ('Started', 'Still running').
        The date column is rewritten relative to the timestamp determined at the first start.

    stop()
        Stops the current running process and returns a status ('Stopped','Not running')

    """

    def __init__(self, hosts, topic, file):
        self.manager = _init_manager()
        self.last_index = Value('i', 0)
        self.silenced = Value(c_bool, False)
        self.offset = None
        self.state = self.manager.Event()
        self.state.set()
        self.hosts = hosts
        self.df = _load_data(file)
        self.topic = topic
        init_topic(hosts, topic)

    def index(self):
        """Returns the last processed index"""
        return self.last_index.value

    def toggle_silenced(self):
        """Suppresses the delivery of the messages to Kafka, but does not stop the process.
        Works as toggle and returns a status
        """
        with self.silenced.get_lock():
            self.silenced.value = not self.silenced.value

        return self.silenced.value if "Not sending Messages" else "Sending again messages"

    def start(self):
        """
        Stars the delivery process and returns a status ('Started', 'Still running').
        The date column is rewritten relative to the timestamp determined at the first start.
        """
        if self.offset is None:
            self.offset = datetime.datetime.fromtimestamp(time.time()) - \
                          dateutil.parser.parse('2008-02-02 08:00:00')

        if not self.state.is_set():
            return 'Still Running'

        self.state = self.manager.Event()
        config = {'bootstrap.servers': self.hosts, "debug": "topic,msg,broker", 'client.id': socket.gethostname()}
        p = Process(target=_run, args=(config, self.topic, self.df, self.state,
                                       self.last_index, self.silenced, self.offset, _msg_callback))
        p.start()
        return 'Started'

    def stop(self):
        """
        Stops the current running process and returns a status ('Stopped','Not running')
        """
        if self.state.is_set():
            return 'Not Running'

        self.state.set()

        return 'Stopped'


def _run(config, topic, df, given_state, given_index, silenced, offset, callback):
    """
    Method to run in a Process to stream a dataframe to Kafka. The loop is running until the Event
    given_state is set or the last element is streamed. The delivery of the messages is suspended when
    the Value silenced is set. The streaming starts with the given index and the message date is set
    relative to the given offset. The messages are send with the given callback.
    """
    producer = Producer(config)
    index = given_index.value
    logging.info("Start with Index " + str(index) + "State: " + str(given_state.is_set()))
    while not given_state.is_set() or df.index[-1] < index:
        row = df.iloc[index]
        new_time = row.loc['date'] + offset
        if (datetime.datetime.fromtimestamp(time.time()) - new_time).total_seconds() > 0:
            row['date'] = new_time
            if not silenced.value:
                producer.produce(topic, key=str(index), value=row.to_json(), callback=callback)
            index = index + 1
            with given_index.get_lock():
                given_index.value = index
        else:
            time.sleep(1 / 100)

    logging.info("Stopped at Index " + str(index))


def init_topic(hosts, topic):
    """
    Creates a new topic with given name on the given kafka cluster

    Attributes
    ----------
    hosts : str
        Kafka connection string
    topic : str
        target topic for Kafka
    """
    client = AdminClient({'bootstrap.servers': hosts})
    fs = client.create_topics([NewTopic(topic, num_partitions=1, config={'log.retention.ms': 4000})])

    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic {} created".format(topic))
        except Exception as e:
            logging.error("Failed to create topic {}: {}".format(topic, e))


def _msg_callback(err, msg):
    """
    Callback for Kafka producer, to handle message errors

    Attributes
    ----------
    err :
        Kafka error message
    msg :
        Kafka message
    """
    if err is not None:
        logging.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        logging.info("Message produced: %s" % (str(msg)))


def _init_manager():
    """
    Initializes the process Manager
    """
    manager = Manager()
    #manager.start()
    return manager


def _load_data(file):
    """
    Utility method to read a pandas data frame from a given path. File must have a date column with a
    timestamp

    Attributes
    ----------
    file : str
        file path
    """
    df = pd.read_csv(file, delimiter=',', parse_dates=True, header=0, low_memory=False)
    df = df.reset_index()
    df.drop(columns=['key'], inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    return df

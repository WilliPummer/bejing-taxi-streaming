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
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance


@singleton
class KafkaManager:
    def __init__(self, hosts, topic, file):
        self.manager = init_manager()
        self.last_index = Value('i', 0)
        self.silenced = Value(c_bool, False)
        self.offset = None
        self.state = self.manager.Event()
        self.state.set()
        self.hosts = hosts
        self.df = load_data(file)
        self.topic = topic
        init_topic(hosts, topic)

    def index(self):
        return self.last_index.value

    def toggle_silenced(self):

        with self.silenced.get_lock():
            self.silenced.value = not self.silenced.value

        return self.silenced.value if "Not sending Messages" else "Sending again messages"

    def start(self):

        if self.offset is None:
            self.offset = time.time()

        if not self.state.is_set():
            return 'Still Running'

        self.state = self.manager.Event()
        config = {'bootstrap.servers': self.hosts, "debug": "topic,msg,broker", 'client.id': socket.gethostname()}
        p = Process(target=run, args=(config, self.topic, self.df, self.state,
                                      self.last_index, self.silenced, self.offset, msg_callback))
        p.start()
        return 'Started'

    def stop(self):

        if self.state.is_set():
            return 'Not Running'

        self.state.set()

        return 'Stopped'


def run(config, topic, df, given_state, given_index, silenced, i_offset, callback):
    producer = Producer(config)
    offset = datetime.datetime.fromtimestamp(i_offset) - dateutil.parser.parse('2008-02-02 08:00:00')
    index = given_index.value
    logging.info("Start with Index " + str(index) + "State: " + str(given_state.is_set()))
    while not given_state.is_set():
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
    client = AdminClient({'bootstrap.servers': hosts})
    fs = client.create_topics([NewTopic(topic, num_partitions=1, config={'log.retention.ms': 4000})])

    for topic, f in fs.items():
        try:
            f.result()
            logging.info("Topic {} created".format(topic))
        except Exception as e:
            logging.error("Failed to create topic {}: {}".format(topic, e))


def msg_callback(err, msg):
    if err is not None:
        logging.error("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        logging.info("Message produced: %s" % (str(msg)))


def init_manager():
    manager = Manager()
    #manager.start()
    return manager


def load_data(file):
    df = pd.read_csv(file, delimiter=',', parse_dates=True, header=0, low_memory=False)
    df = df.reset_index()
    df.drop(columns=['key'], inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    return df

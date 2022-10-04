import os
import sys

from flask import Flask, jsonify, redirect, url_for
from waitress import serve
import kafkaManager as km
import logging

app = Flask(__name__)

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

hosts = os.environ['KAFKA_HOSTS']
topic = os.environ['KAFKA_TOPIC']
logging.info(f'Connecting to {topic} on hosts {hosts}')


@app.route('/start', methods=['POST'])
def start_process():
    logging.info('Start Producer')
    status = get_manager().start()
    return jsonify(status=status)


@app.route('/silence', methods=['POST'])
def silence():
    logging.info('Silencing producing')
    status = get_manager().toggle_silenced()
    return jsonify(status=status)


@app.route('/stop', methods=['POST'])
def stop_process():
    logging.info('Stop Producer')
    status = get_manager().stop()
    return jsonify(status=status)


@app.route('/index', methods=['GET'])
def index():
    return jsonify(index=get_manager().index())


@app.route('/')
def hello_geek():
    return redirect(url_for('static', filename='index.html'))


def get_manager():
    return km.KafkaManager(hosts, topic, 'data/P-TEST.csv')


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=5000)

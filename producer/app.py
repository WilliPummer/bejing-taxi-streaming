import os
import sys

from flask import Flask, jsonify, redirect, url_for
from waitress import serve
import kafkaManager as km
import logging

app = Flask(__name__)

# Configure logging to log to stdout
root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

# read environment variables
hosts = os.environ['KAFKA_HOSTS']
topic = os.environ['KAFKA_TOPIC']
logging.info(f'Connecting to {topic} on hosts {hosts}')


@app.route('/start', methods=['POST'])
def start_process():
    """
    Start the message processing
    """
    logging.info('Start Producer')
    status = get_manager().start()
    return jsonify(status=status)


@app.route('/silence', methods=['POST'])
def silence():
    """
    Suspense the message processing, but keeps the message loop running
    """
    logging.info('Silencing producing')
    status = get_manager().toggle_silenced()
    return jsonify(status=status)


@app.route('/stop', methods=['POST'])
def stop_process():
    """
    Stops the message processing
    """
    logging.info('Stop Producer')
    status = get_manager().stop()
    return jsonify(status=status)


@app.route('/index', methods=['GET'])
def index():
    """
    Delivers the last processed index
    """
    return jsonify(index=get_manager().index())


@app.route('/')
def init_page():
    """
    Renders index page
    """
    return redirect(url_for('static', filename='index.html'))


def get_manager():
    """
    Creates and returns a new KafkaManger
    """
    return km.KafkaManager(hosts, topic, 'data/P-TEST.csv')


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=5000)

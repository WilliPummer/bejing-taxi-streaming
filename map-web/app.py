import logging
import os
import sys
import time
import uuid

import pandas as pd
import kafkaConsumer as kc

from flask import Flask, Response, redirect, url_for
from waitress import serve

app = Flask(__name__)

df = pd.read_csv('files/P-TESTy.csv', index_col=0)

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

@app.route('/')
def hello_world():
    return redirect(url_for('static', filename='index.html'))


@app.route('/stream')
def generate_large_csv():
    def generate():
        listener = kc.KafkaListener(hosts, uuid.uuid4().hex, [topic])
        for msg in listener.consume():
            time.sleep(.01)
            data = f'{{"id": "{msg["id"]}", "lat": "{msg["lat"]}", "lng": "{msg["lon"]}"}}'
            yield "data: %s\n\n" % data

    return Response(generate(), content_type='text/event-stream')


if __name__ == '__main__':
    serve(app, host="0.0.0.0", port=5001)

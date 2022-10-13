from datetime import datetime
import logging
import os
import sys
import uuid
import kafkaConsumer as kc
from flask import Flask, Response, redirect, url_for, stream_with_context

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


@app.route('/')
def hello_world():
    return redirect(url_for('static', filename='index.html'))


@app.route('/stream')
def generate_large_csv():
    def generate():
        listener = kc.KafkaListener(hosts, uuid.uuid4().hex, [topic])
        try:
            for msg in listener.consume():
                logging.info(msg)
                if isinstance(msg, str):
                    yield "data: %s\n\n" % msg
                else:
                    date = datetime.fromtimestamp(msg["date"]/1000).strftime("%H:%M:%S")
                    data = f'{{"id": "{msg["id"]}","date": "{date}", "lat": "{msg["lat"]}", "lng": "{msg["lon"]}"}}'
                    yield "data: %s\n\n" % data
        except GeneratorExit:
            logging.info("Closed")
            listener.close_consumer()

    return Response(stream_with_context(generate()), content_type='text/event-stream')

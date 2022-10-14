import json
from datetime import datetime
import logging
import os
import sys
import uuid

import requests

import kafkaConsumer as kc
from flask import Flask, Response, redirect, url_for, stream_with_context, render_template

app = Flask(__name__)

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

dist_url = os.environ['DIST_URL']
hosts = os.environ['KAFKA_HOSTS']
topic = os.environ['KAFKA_TOPIC']

logging.info(f'Connecting to {topic} on hosts {hosts}')


@app.route('/')
def hello_world():
    return redirect(url_for('static', filename='index.html'))


@app.route('/table')
def table():
    response = requests.get(dist_url)

    try:
        response = requests.get(dist_url, timeout=5)
        response.raise_for_status()
        json = response.json()

        for element in json:
            element['dis'] = '{:.2f} km'.format(element['dis'] / 1000)
            try:
                element['date'] = datetime.fromtimestamp(element['date']/1000.0).strftime("%d/%m/%Y, %H:%M:%S")
            except ValueError:
                logging.info(element)

        return render_template('table.html', title='Distance Table',
                               entries=json)

    except requests.exceptions.HTTPError as errh:
        print(errh)
    except requests.exceptions.ConnectionError as errc:
        print(errc)
    except requests.exceptions.Timeout as errt:
        print(errt)
    except requests.exceptions.RequestException as err:
        print(err)


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
                    date = datetime.fromtimestamp(msg["date"] / 1000).strftime("%H:%M:%S")
                    data = f'{{"id": "{msg["id"]}","date": "{date}", "lat": "{msg["lat"]}", "lng": "{msg["lon"]}"}}'
                    yield "data: %s\n\n" % data
        except GeneratorExit:
            logging.info("Closed")
            listener.close_consumer()

    return Response(stream_with_context(generate()), content_type='text/event-stream')

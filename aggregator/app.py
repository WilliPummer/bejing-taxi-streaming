import datetime as dt
import logging
import os
import sys
import faust
import numpy as np

# set logging to stdout
root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

# init environment variables
agg_name = os.environ['AGG_NAME']
broker_url = os.environ['BROKER_URL']
topic = os.environ['KAFKA_TOPIC']
table_name = os.environ['TABLE_BASE_NAME']

logging.info(f'Starting {agg_name}: Connecting to topic {topic} on {broker_url} with table {table_name}')

# setup
app = faust.App(agg_name, broker=broker_url, version=1, topic_partitions=1)
source = app.topic(topic, value_type=str)
dists = (app.Table(table_name, default=list, partitions=10))


@app.agent(source)
async def listen_to_stream(stream):
    """
    Reads the configured stream and stores for each event the latest distance by taxi id in a table

    :param stream: topic stream to iterate
    :return: None
    """
    logging.info("Listening to stream")
    async for event in stream:
        value_list = dists[event['id']]

        if len(value_list) != 0:
            last_ele = [x for x in value_list if x['valid']][-1]
            new_dis = haversine(last_ele['lat'], last_ele['lon'], event['lat'], event['lon'])
            is_valid = valid_point(last_ele['date'], event['date'], new_dis)
            value_list.append(_to_dict(last_ele, event, is_valid, new_dis))
        else:
            value_list.append(_event_to_dict(event))

        dists[event['id']] = value_list


def _event_to_dict(event):
    return dict({'key': event['id'], 'lat': event['lat'], 'lon': event['lon'],
                 'date': event['date'], 'dis': 0, 'valid': bool(True)})


def _to_dict(last, event, is_valid, dist):
    return dict({'key': last['key'], 'lat': event['lat'], 'lon': event['lon'], 'date': event['date'],
                 'dis': (last['dis'] + dist), 'valid': bool(is_valid)})


def valid_point(time_a, time_b, distance, max_speed=120):
    """
    Validates if the distances between the timeframe of two points is within the maximum allowed range.

    :param time_a: time of data point A
    :param time_b: time of data point B
    :param distance: distance between the two points
    :param max_speed: maximum allowed speed between two points, (default set to max speed in China)
    :return: True if within allowed range, False otherwise
    """
    total_seconds = (dt.datetime.fromtimestamp(time_b/1000) - dt.datetime.fromtimestamp(time_a/1000)).total_seconds()
    max_allowed_dist = (max_speed*1000)/3600*total_seconds
    logging.info(f' Total seconds: {total_seconds} -> allowed dist {max_allowed_dist} | actual dist {distance}')
    return distance < max_allowed_dist


@app.page('/{taxi}/distance')
@app.table_route(table=dists, match_info='taxi')
async def get_count(web, request, taxi):
    """
    Returns the latest distance of the given taxis since start of the stream as json
    """
    return web.json({
        int(taxi): dists[int(taxi)],
    })


@app.page('/distance/latest')
async def get_count(web, request):
    """
    Returns the latest distance of all driven taxis since start of the stream as json
    """
    result = []
    for key in dists:
        result.append(dists[key][-1])

    return web.json(result)


def haversine(lat1, lon1, lat2, lon2):
    """
        Copy of: https://stackoverflow.com/a/29546836

    Calculate the great circle distance between two points in meter
    on the earth (specified in decimal degrees)

    All args must be of equal length

    :param lat1: latitude of point 1
    :param lon1: longitude of point 1
    :param lat2: latitude of point 2
    :param lon2: longitude of point 2
    :return: distance in meter
    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    a = np.sin((lat2-lat1)/2.0)**2 + \
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    return (6371 * 2 * np.arcsin(np.sqrt(a)))*1000


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
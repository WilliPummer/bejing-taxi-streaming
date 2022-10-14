import logging
import os
import sys
import faust
import numpy as np

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

CLEANUP_INTERVAL = 1.0
WINDOW = 10
WINDOW_EXPIRES = 1
PARTITIONS = 1

agg_name = os.environ['AGG_NAME']
broker_url = os.environ['BROKER_URL']
topic = os.environ['KAFKA_TOPIC']
table_name = os.environ['TABLE_BASE_NAME']

logging.info(f'Starting {agg_name}: Connecting to topic {topic} on {broker_url} with table {table_name}')

app = faust.App(agg_name, broker=broker_url, version=1, topic_partitions=1)
source = app.topic(topic, value_type=str)
dists = (app.Table(table_name, default=list, partitions=PARTITIONS))


@app.agent(source)
async def print_windowed_events(stream):
    logging.info("Listening to stream")
    async for event in stream:
        value_list = dists[event['id']]

        if len(value_list) != 0:
            last_ele = value_list[-1]
            new_dis = haversine(last_ele['lat'], last_ele['lon'], event['lat'], event['lon'])
            value_list.append(dict({'key': last_ele['key'], 'lat': event['lat'], 'lon': event['lon'], 'date': event['date'], 'dis': (last_ele['dis'] + new_dis)}))
        else:
            value_list.append(dict({'key': event['id'], 'lat': event['lat'], 'lon': event['lon'], 'date': event['date'], 'dis': 0}))

        dists[event['id']] = value_list


@app.page('/{taxi}/distance')
@app.table_route(table=dists, match_info='taxi')
async def get_count(web, request, taxi):
    return web.json({
        int(taxi): dists[int(taxi)],
    })


@app.page('/distance/latest')
#@app.table_route(table=dists, match_info='taxi')
async def get_count(web, request):

    result = []
    for key in dists:
        result.append(dists[key][-1])

    return web.json(result)


#https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas/29546836#29546836
#https://stackoverflow.com/questions/40452759/pandas-latitude-longitude-to-distance-between-successive-rows
def haversine(lat1, lon1, lat2, lon2, to_radians=True, earth_radius=6371):
    """
    slightly modified version: of http://stackoverflow.com/a/29546836/2901002

    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees or in radians)

    All (lat, lon) coordinates must have numeric dtypes and be of equal length.

    """

    if to_radians:
        lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    a = np.sin((lat2-lat1)/2.0)**2 + \
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2-lon1)/2.0)**2

    return (earth_radius * 2 * np.arcsin(np.sqrt(a)))*1000


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
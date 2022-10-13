import json
import math
from datetime import datetime, timedelta
from time import time
import random
import faust
import pandas as pd
import numpy as np


CLEANUP_INTERVAL = 1.0
WINDOW = 10
WINDOW_EXPIRES = 1
PARTITIONS = 1

app = faust.App('taxi-agg', broker="kafka://localhost:39092;localhost:29092", version=1, topic_partitions=1)

#app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic("test", value_type=str)
sink = app.topic("agg-event", value_type=str)

dists = (app.Table('dists', default=list, partitions=PARTITIONS))


@app.agent(source)
async def print_windowed_events(stream):
    async for event in stream:
        value_list = dists[event['id']]

        if len(value_list) != 0:
            last_ele = value_list[-1]
            print(last_ele)
            new_dis = haversine(last_ele['lat'], last_ele['lon'], event['lat'], event['lon'])
            value_list.append(dict({'lat': event['lat'], 'lon': event['lon'], 'date': event['date'], 'dis': (last_ele['dis'] + new_dis)}))
        else:
            value_list.append(dict({'lat': event['lat'], 'lon': event['lon'], 'date': event['date'], 'dis': 0}))

        dists[event['id']] = value_list


@app.page('/{taxi}/distance')
@app.table_route(table=dists, match_info='taxi')
async def get_count(web, request, taxi):
    return web.json({
        int(taxi): dists[int(taxi)],
    })

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
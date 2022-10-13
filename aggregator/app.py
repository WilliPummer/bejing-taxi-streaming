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


class Entry(faust.Record, serializer='json'):
    id: int
    key: int
    date: datetime
    lat: str
    lon: str

app = faust.App('taxi-agg', broker="kafka://localhost:39092;localhost:29092", version=1, topic_partitions=2)

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic("test", value_type=Entry)
sink = app.topic("agg-event", value_type=str)





def window_processor(key, events):
    start = datetime.fromtimestamp(key[1][0]).strftime('%H:%M:%S')
    end = datetime.fromtimestamp(key[1][1]).strftime('%H:%M:%S')
    print(f'Start: {start}, End: {end}')

    dict = {}

    df = pd.DataFrame(events)
    print(df.dtypes)

    for i, g in df.groupby(['id']):
        g['distance'] = haversine(g.lat.shift(), g.lon.shift(), g.loc[1:, 'lat'], df.loc[1:, 'lon'])
        last_dist = g.loc[g.index[-1], "distance"]
        if not math.isnan(last_dist):
            dict[i] = last_dist

    print(dict)

    #sink.send_soon(value=AggModel(date=timestamp, count=count, mean=mean))


tumbling_table = (
    app.Table(
        'stats',
        default=list,
        partitions=PARTITIONS,
        on_window_close=window_processor,
    )
    .tumbling(timedelta(minutes=1), expires=timedelta(minutes=1)).relative_to_field()

)


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




@app.agent(source)
async def print_windowed_events(stream):
    async for event in stream:
        value_list = tumbling_table['events'].value()
        value_list.append(event)
        tumbling_table['events'] = value_list

if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.argv.extend(['worker', '-l', 'info'])
    app.main()
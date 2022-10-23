## Introduction
This use case exemplifies the usage of Kafka as a streaming infrastructure to stream GPS data to a web application. The 
use case is composed of the following parts

#### Infrastructure nodes
The Kafka infrastructure is composed of two zookeeper nodes and two Kafka nodes to form a minimal cluster. 

#### Producer
The producer is a small Flask web application that streams the taxi GPS data into a Kafka topic. Additionally the app 
resets the timestamp to the current time when starting the stream to simulate 'real-time.' The UI is reachable unde

* localhost:8000

It is possible to start, stop and silence the data streaming. Data points will be processed but not sent to the topic 
when silenced. Per default, it creates a topic with ten partitions.

#### Aggregator
The aggregator is a Faust-streaming application that aggregates the driven distance by taxi by calculating the haversine 
distance between two points. The new point is deemed an outlier if the speed between two points exceeds 120 km/h. The 
application exposes the data over a REST API:

* localhost:8002/{taxi}/distance - Returns all data points for a given taxi
* localhost:8002/distance/latest - Returns the latest distance for all taxis

The aggregation is done over a Kafa table that is backed by a changelog topic. It must have the same partions as the 
main topic (10 per default).

#### Map-web
The map-web application offers two views a live map that displays the position of the taxis and a table view that shows 
the total driven distance by taxi. The map view shows green dots for moving taxis and red dots if they are not moving 
for more than one minute. 

* localhost:8002/ - Live Map view
* localhost:8002/table -  Total distance by taxi

For each active tab, the live view will open a new consumer that starts with the latest offset.

### Dataset
The basis for this use case is a subset of the T-Drive trajectory dataset (https://www.microsoft.com/en-us/research/publication/t-drive-trajectory-data-sample/), 
consisting of 15 million GPS data points of about 10,000 taxis in Beijing. The subset uses only taxis with a sampling 
rate of a minimum of five seconds, about 1.7 million data points.

Each one-week taxi data set was split into multiple groups spanning one day to raise the data density.

### Usage

After cloning the repository, the whole stack can be started over the command:

    docker compose up

The producer must be started manually (Start button on localhost:8000).

### Used Resources

* https://stackoverflow.com/a/29546836 (haversine function)
* https://www.baeldung.com/ops/kafka-docker-setup (kafka setup)
* https://faust-streaming.github.io/faust/ (faust setup)
* https://leafletjs.com/ (map frontend)

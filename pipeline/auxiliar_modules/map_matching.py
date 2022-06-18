import numpy as np
import requests
import json 
import pandas as pd;
import requests
from tables.measurements import filter_measurements, format_measurements, format_for_map_matching;
from auxiliar_modules.db_queries import get_computed_ways;

from auxiliar_modules.auxiliar_classes import Node;
from auxiliar_modules.auxiliar_classes import Way;



def overpass_query(query):
    """
    Executes a query to the LiRA Overpass server and returns its result.

    :param query: Query to execute
    :type query: str

    """
    url = "http://lira-osm.compute.dtu.dk/api/interpreter?data=[timeout:180][out:json];"
    result = requests.get(url + query, headers={'Content-type': 'application/json'})
    return json.loads(result.text)

def get_nodes_and_ways_from_way_ids(way_ids):

    """
    Gets OSM Ways and Nodes based on the list of way ids passed as a parameter using the LiRA Overpass server.

    :param way_ids: Ids of the ways and its nodes to get.
    :type way_ids: List[int]

    """

    ids = ','.join([str(id) for id in way_ids])
    query = f"""way[highway](id:{ids});
    out geom;
    node(w);
    out;"""


    result = overpass_query(query);
    elements = result["elements"];
    nodes = [];
    ways = [];
    
    for element in elements:
        if element["type"] == "node":
            node = Node(element["id"], element["lat"], element["lon"]);
            nodes.append(node);
        elif element["type"] == "way":
            way = Way(element["id"], element["nodes"]);
            ways.append(way);
    return nodes, ways;

def req_valhalla_service(chunk):
    """
    Requests the use of the Valhalla Service which performs the map matching.

    :param chunk: Chunk of data points to be map matched.
    :type chunk: List[[float, float, int]]

    """
    url = 'https://valhalla1.openstreetmap.de/trace_attributes' 
    df = pd.DataFrame(data={
        "lon": [float(p[1]) for p in chunk], 
        "lat": [float(p[0]) for p in chunk],
        "time": [int(p[2]) for p in chunk]
    })
    
    coords = df.to_json(orient='records')
    data = '{"shape":' + str(coords) + ""","shape_match":"map_snap", "use_timestamps": "true","costing":"auto", "format":"osrm", "trace_options":{"search_radius":20}}"""
    res = requests.post(url, data=data, headers={'Content-type': 'application/json'})
    print(res);
    return json.loads(res.text)



def map_match(data: np.ndarray):
    print(len(data));
    chunk = req_valhalla_service(data)
    way_ids = [edge['way_id'] for edge in chunk['edges']]
    map_matched_points = chunk['matched_points'];
        
    return way_ids, map_matched_points


def map_match_chunk(measurements):
    """
    Map matches a chunk of measurements.

    :param measurements: List of measurements to be map matched.
    :type measurements: List[Measurement]

    """
    measurements_for_map_matching = format_for_map_matching(measurements);
    map_matched_way_ids, map_matched_measurements = map_match(measurements_for_map_matching);
   
    nodes, ways = get_nodes_and_ways_from_way_ids(map_matched_way_ids);
    way_ids = [way.id for way in ways];

    res = [];
    last_edge_index = 0;
    i = 0;
    for measurement in measurements:
        matched_measurement = map_matched_measurements[i]
        if matched_measurement['type'] == 'unmatched':
                i = i + 1;
                continue;

        edge_index = matched_measurement['edge_index'];

        cur_edge_index = edge_index 
        if edge_index >= len(map_matched_way_ids):
            cur_edge_index = last_edge_index
        else:
            last_edge_index = cur_edge_index;

        measurement.position = [float(matched_measurement['lat']), float(matched_measurement['lon'])];
        measurement.way = map_matched_way_ids[cur_edge_index];

        if measurement.way not in way_ids:
            continue;
        res.append(measurement)
        i = i + 1;

    return res, ways, nodes;


def format_and_map_match_measurements(measurements_from_db):
    """
    Formats and separates the measurements from the LiRA database in chunks and map matches them separately.

    :param measurements_from_db: List of measurements to be map matched.
    :type measurements_from_db: List[[]]

    """
    formatted_measurements = format_measurements(filter_measurements(measurements_from_db));
    measurements = []
    nodes = [];
    ways = [];
    chunks = [formatted_measurements[x:x+15000] for x in range(0, len(formatted_measurements), 15000)]

    for chunk in chunks: 
        measurements_of_chunk, ways_of_chunk, nodes_of_chunk = map_match_chunk(chunk);
        
        measurements = measurements + measurements_of_chunk;
        nodes = nodes + nodes_of_chunk;
        ways = ways + ways_of_chunk;

    return measurements, get_unique_nodes(nodes), get_unique_ways(ways);


def classify_ways(way_ids):
    """
    For a set of way ids, it divides it between computed ways that are already stored in the visualization database
    and not computed ways that need to be inserted for the first time.

    :param way_ids: List of way ids to be classified.
    :type way_ids: List[int]

    """
    computed_ways = get_computed_ways();

    for i in range(0, len(computed_ways)):
        computed_ways[i] = computed_ways[i][0];

    not_computed_ways = [];
    for id in way_ids:
        if id not in computed_ways:
            not_computed_ways.append(id);

    return computed_ways, not_computed_ways;


def get_unique_nodes(nodes):
    res = [];
    nodes_ids = [];
    for node in nodes:
        if node.id not in nodes_ids:
            nodes_ids.append(node.id);
            res.append(node);
    return res;

def get_unique_ways(ways):
    res = [];
    ways_ids = [];
    for way in ways:
        if way.id not in ways_ids:
            ways_ids.append(way.id);
            res.append(way);
    return res;

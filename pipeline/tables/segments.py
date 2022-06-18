
from auxiliar_modules.auxiliar_classes import Node, Way
from auxiliar_modules.db_queries import get_segments_from_ways;
import geopy;
import geopy.distance
from auxiliar_modules.auxiliar_classes import NodesDictionary, WaysDictionary;
from auxiliar_modules.db_queries import connect;
import random;
from typing import List, Dict

class Segment:
    """This is a conceptual class representation of a Segment.

    :param id: The id of the Segment.
    :type id: int
    :param position_a: Position (latitude and longitude) of the first node of the Segment.
    :type position_a: List[float]
    :param position_b: Position (latitude and longitude) of the second node of the Segment.
    :type position_b: List[float]
    :param length: Length of the Segment in kilometers.
    :type length: float
    :param way_id: Id of the way to which the Segment pertains
    :type way_id: int
    """
    def __init__(self, id, position_a, position_b, length, way_id):
        self.id: int = id;
        self.position_a: List[float] = position_a;
        self.position_b: List[float] = position_b;
        self.length: float = length;
        self.way: int = way_id;

    def get_db_row(self) -> List:
        """Returns a list of values to be inserted in the visualization database as a Segment
        """

        position_a = 'POINT(' + str(self.position_a[0]) + ' ' + str(self.position_a[1]) + ')';
        position_b = 'POINT(' + str(self.position_b[0]) + ' ' + str(self.position_b[1]) + ')';
        return [self.id, position_a, position_b, self.length, self.way]

def parse_segments(segment_rows: List) -> List[Segment]:
    """
    Converts segment rows retrieved from the database to segment objects to be used within the pipeline

        :param segment_rows: A list of values representing a Segment object
        :type segment_rows: List[]
    """
    segments = [];
    for row in segment_rows:
        positionA = (row[5], row[6]);
        positionB = (row[7], row[8])
        segment = Segment(row[0], positionA, positionB, row[3], row[4])
        segments.append(segment);

    return segments;



class Segments(object):
    
    """This is a conceptual class representation of the Segments table. It stores the segments that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    segments: List[Segment] = [];

    segments_per_id: Dict = {};
    segments_per_way: Dict = {};

    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Segments, cls).__new__(cls, *args, **kwargs)
        return cls._instance



    def compute_data(self, computed_ways: List[int], not_computed_ways: List[int]):
        """Computes, creates and stores Segment objects in the class instance based on the ways passed as parameters.

        :param computed_ways: Ways whose segments need to be retrieved from the database instead of computed.
        :type computed_ways: List[int]
        :param not_computed_ways: Ways that have never been computed and whose segments need to be inserted in the visualization database.
        :type not_computed_ways: List[int]
        """
        self.segments_to_insert = self.compute_ways_and_generate_segments(not_computed_ways);
        self.segments_in_db = parse_segments(get_segments_from_ways(computed_ways));
        self.segments = self.segments_to_insert + self.segments_in_db;
        self.generate_dictionaries(self.segments);
        pass

    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.segments = [];
        self.segments_per_id = {};
        self.segments_per_way = {};



    #### DATABASE #####
    def insert_into_db(self):
        """
        Inserts the Segments into the Segments table in the visualization database. 
        """
        print("Inserting segments in db")

        segments = self.segments_to_insert;
        if len(segments) == 0:
            return;

        rows = [];
        for segment in segments:
            if segment != None:
                rows.append(segment.get_db_row());

        cur, conn = connect();

        args_str = ','.join("({id},'{posA}','{posB}',{Length},{Way})"
        .format(id = x[0], posA = x[1], posB = x[2], Length = x[3], Way = x[4]) for x in rows)

        sql = """
            INSERT INTO "segments"("id","position_a", "position_b", "length", "way") VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;


    #### DICTIONARY GENERATION ####


    def generate_dictionaries(self, computed_segments: List[Segment]):
        """
        Computes dictionaries that store the segments. The purpose of the dictionaries is for segments to be
        accessed and searched in a most efficient way.

        :param computed_segments: Segments to be inserted into the dictionaries.
        :type computed_segments: List[Segment] 
        """
        computed_ways = [];
        for segment in computed_segments:
            self.segments_per_id[segment.id] = segment;
            way = segment.way;
           
            if way in computed_ways:
                self.segments_per_way[way].append(segment);
            else:
                self.segments_per_way[way] = [segment];
                computed_ways.append(way);

    

    #### DICTIONARY NAVIGATION METHODS ####

    def get_segments_in_a_way(self, way_id: int) -> List[Segment]:
        """
        Returns all Segments that pertain to a certain way

        :param way_id: Way id of the way to which the segments must pertain.
        :type way_id: List[Segment]
        """
        return self.segments_per_way[way_id];
    
    def get_segments_by_id(self, seg_id: int) -> Segment:
        """
        Returns the Segment that is identified by a certain id.

        :param seg_id: Id of the segment
        :type seg_id: int
        """
        return self.segments_per_id[seg_id];
        
    
    #### SEGMENT COMPUTATION ####

    # Calculates distance between 2 geopoints.
    def calculate_distance(self, point_a, point_b):
        """
        Returns the distance in kilometers between two geographical points.

        :param point_a: Latitude and longitude of the first geographical point.
        :type point_a: List[float]
        :param point_b: Latitude and longitude of the second geographical point.
        :type point_b: List[float]
        """
        a = (point_a[0], point_a[1]);
        b = (point_b[0], point_b[1]);
        return geopy.distance.distance(a, b).km;

    def generate_id(self, way: Way, node_a: Node, node_b: Node) -> int:
        """
        Generates a unique id based on the attributes of a segment

        :param way: Way to which the segment pertains.
        :type way: :class Way
        :param node_a: First node of the segment.
        :type node_a: :class Node
        :param node_b: Second node of the segment.
        :type node_b: :class Node
        """
        way_id_str = str(way.id);
        node_a_str = str(node_a.id);
        node_b_str = str(node_b.id);

        segment_id = way_id_str[len(way_id_str) - 3:] + node_a_str[len(node_a_str) - 3:] + node_b_str[len(node_b_str) - 3:] 
        return int(segment_id);


    # Divides a way into different segments
    def compute_way(self, way: Way) -> List[Segment]:

        """
        Returns the computed segments for a certain way

        :param: way: Way to be computed.
        :type way: List[:class Segment]
      
        """

        # <overpy.Way id=1880634 nodes=[8082256, 8082270, 1377590132, 8082258, 1659103599, 8082259, 8082260, 1659103318, 8082261, 1605138498, 294211770, 1038008328]>
        # <overpy.Node id=125436 lat=55.7207330 lon=12.5437336>
        way_nodes = way.nodes;
        res = [];
        node_dictionary = NodesDictionary();
        for x in range(len(way_nodes) - 1):
            node_a = node_dictionary.get_node_by_id(way_nodes[x]);
            node_b = node_dictionary.get_node_by_id(way_nodes[x + 1]);
            position_a = [float(node_a.lat), float(node_a.lon)];
            position_b = [float(node_b.lat), float(node_b.lon)];
            
            segment = Segment(self.generate_id(way, node_a, node_b), position_a, position_b, self.calculate_distance(position_a, position_b), way.id);
            res.append(segment);

        return res;


    # For each way generates it's segments and returns the list ready to insert into the db
    def compute_ways_and_generate_segments(self, way_ids: List[int]) -> List[Segment]:
        """
        Returns the computed segments for a list of way ids.

        :param: way_ids: Ids of the ways to be computed.
        :type way_ids: List[int]
      
        """
        segments_list = [];
        way_dictionary = WaysDictionary();
        if len(way_ids) != 0:
            for way_id in way_ids:
                segments_list = segments_list + self.compute_way(way_dictionary.get_way_by_id(way_id));
            
        return segments_list;




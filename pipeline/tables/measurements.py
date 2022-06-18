from datetime import datetime
from typing import Dict, List

from numpy import number

from tables.segments import Segments, Segment;
from auxiliar_modules.db_queries import connect;
import geopy;
import geopy.distance
import json;
import time;

class Measurement:
    """This is a conceptual class representation of a car sensor measurement.

    :param id: The id of the measurement.
    :type id: str
    :param type: Type of the measurement. Ex: obd.trac_cons.
    :type type: str
    :param position: Position (latitude and longitude) of measurement.
    :type position: List[float]
    :param value: Value of the measurement.
    :type value: float
    :param trip: Id of the trip to which the measurement pertains
    :type trip: str
    :param created_at: Timestamp of when was the measurement measured.
    :type created_at: str
    :param updated_at: Timestamp of when was the measurement updated in the LiRA database.
    :type updated_at: str
    :param segment: Segment to which the measurement pertains.
    :type segment: Segment
    :param direction: Direction in which the car was going at the time of the measure.
    :type direction: int
    :param way: Id of the way to which the measurement pertains.
    :type way: int
    """


    def __init__(self, id, type, position, value, trip, created_at, updated_at, segment):
        self.id: str = id;
        self.type: str = type;
        self.position: List[float] = position;
        self.value: float = value;
        self.trip: str = trip;
        self.created_at = created_at;
        self.updated_at = updated_at;
        self.segment: Segment = segment;
        self.direction: int = None;
        self.way: int = None;

    def get_db_row(self):
        """Returns a list of values to be inserted in the visualization database as a Measurement
        """

        position = 'POINT(' + str(self.position[0]) + ' ' + str(self.position[1]) + ')';
        return [self.id, self.type, position, self.value, self.trip, self.created_at, self.updated_at, self.segment.id, self.direction];

def filter_measurements(measurements: List):
    """
    Reduces the amount of measurements to compute

    :param measurements: Original list of measurements to compute
    :type measurements: List[]
    """

    res = [];
    i = 0;

    number_of_measurements = len(measurements);
    if(number_of_measurements > 80000):
        filter_coefficient = 10;
    else:
        filter_coefficient = 1;


    for measurement in measurements:
        if i % filter_coefficient == 0 or measurement[2] == 'obd.trac_cons':
            if(measurement[3] != None and measurement[4] != None):
                res.append(measurement);
        i = i + 1;
    return res;

def format_for_map_matching(measurements: List[Measurement]) -> List:
    """
    Returns a list of measurements ready for map matching based on the measurements passed as a parameter.

    :param measurements: List of measurements to format.
    :type measurements: List[Measurement]

    """

    measurementsPos = [];
    for m in measurements:
            timestamp = m.created_at;

            if type(timestamp) is str:
                timestamp = datetime.strptime(timestamp.split('+')[0], '%Y-%m-%d %H:%M:%S.%f')
            timestamp = time.mktime(timestamp.timetuple())
            mPos = [m.position[0], m.position[1], timestamp];
            measurementsPos.append(mPos);
    return measurementsPos;



def format_3d_measurement(measurement: List, type: str):
    """
    Returns three formatted measurements, one for each dimension. Just for measurements
    which have 3 dimensions such as acceleration (acc.xyz)
    :param measurement: List of values representing a measurement
    :type measurement: List[]

    """
    message = json.loads(measurement[5]);

    if type+'.x' in message:
        value_x = message[type + '.x'];
        # value_y = message[type + '.y'];
        # value_z = message[type + '.z'];

        # print(type+'.x' + " " + str(value_x));
        # print(type+'.y' + " " + str(value_y));
        # print(type+'.z' + " " + str(value_z));

        position = [measurement[3], measurement[4]]
        mesX = Measurement(measurement[0], type + '.x', position, value_x, measurement[7], measurement[9], measurement[10], -1)
        # mesY = Measurement(measurement[0], type + '.y', position, value_y, measurement[7], measurement[9], measurement[10], -1)
        # mesZ = Measurement(measurement[0], type + '.z', position, value_z, measurement[7], measurement[9], measurement[10], -1)

        return [mesX];
    return [];


def format_2d_measurement(measurement: List, type: str):
    """
    Returns a formatted measurement. Just for measurements
    which have 1 dimensions such as power consumption (obd.trac_cons)
    :param measurement: List of values representing a measurement
    :type measurement: List[]

    """
    message = json.loads(measurement[5]);

    if type + '.value' in message:
        value = message[type + '.value'];
        
        position = [measurement[3], measurement[4]]
        mes = Measurement(measurement[0], type, position, value, measurement[7], measurement[9], measurement[10], -1)
        return [mes];

    return [];


def format_measurements(measurements: List):
    """
    Takes measurements that have been retrieved from the LiRA database and formats them into Measurement class instances.
    :param measurement: List of measurements
    :type measurement: List[]

    """
    res = [];
    for row in measurements:
        type = row[2];
        parts = type.split('.');
        if parts[1] == 'xyz':
            res = res + format_3d_measurement(row, type);
        else:
            res = res + format_2d_measurement(row, type);

    return res;



def parse_measurements(measurements_rows: List) -> List[Measurement]:
    """
    Takes measurements that have been retrieved from the visualization database and formats them into Measurement class instances.
    :param measurement: List of measurements
    :type measurement: List[]

    """
    measurements = [];
    for row in measurements_rows:
        position = (row[8], row[9]);
        measurement = Measurement(row[0], row[1], position, row[3], row[4], row[5], row[6], row[7])
        measurements.append(measurement);

    return measurements;



class Measurements(object):

    """This is a conceptual class representation of the Measurements table. It stores the measurements that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    measurements: List[Measurement] = [];

    measurements_per_type: Dict = {};
    measurements_per_segment: Dict = {};
    segment_of_measurement: Dict = {};
    measurements_per_id: Dict = {};

    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Measurements, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    
    def compute_data(self, measurements: List[Measurement]):
        """Assigns a segment and a direction to each measurement passed as parameter. Afterwards it stores them as a list and dictionaries
        in the class instance.

        :param measurements: Measurements that need to be assigned to a segment.
        :type measurements: List[Measurement]
        
        """
        print("computing measurements for a trip")
        computed_measurements = self.compute_measurements(measurements)
        self.measurements = computed_measurements;
        print("generating dictionaries")
        self.generate_dictionaries(computed_measurements)
        print("computing directions")
        self.compute_directions();
        

    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.measurements = [];
        self.measurements_per_type = {};
        self.measurements_per_segment = {};
        self.segment_of_measurement = {};
        self.measurements_per_id = {};


    #### DATABASE ####

    def insert_into_db(self):
        """
        Inserts the Measurements into the Measurements table in the visualization database. 
        """
        measurements = self.measurements;
        if len(measurements) == 0:
            return;

        rows = [];
        for measurement in measurements:
            if measurement != None:
                rows.append(measurement.get_db_row());        

        cur, conn = connect();

        args_str = ','.join("('{Id}','{Type}','{Position}',{Value},'{Trip}','{Created}','{Updated}',{Segment}, {Direction})"
        .format(Id = x[0], Type = x[1], Position = x[2], Value = x[3], Trip = x[4], Created = x[5], Updated = x[6], Segment = x[7], Direction = x[8]) for x in rows)

        sql = """
            INSERT INTO "measurements" VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;


    #### DICTIONARY GENERATION ####

    def generate_dictionaries(self, computed_measurements: List[Measurement]):
        """
        Computes dictionaries that store the measurements. The purpose of the dictionaries is for measurements to be
        accessed and searched in a most efficient way.

        :param computed_measurements: Measurements to be inserted into the dictionaries.
        :type computed_measurement: List[Measurement] 
        """
        computed_types = [];
        computed_segments = [];
        for measurement in computed_measurements:

            self.segment_of_measurement[measurement.id] = measurement.segment;
            self.measurements_per_id[measurement.id] = measurement;

            type = measurement.type;
            if type in computed_types:
                self.measurements_per_type[type].append(measurement);
            else:
                self.measurements_per_type[type] = [measurement];
                computed_types.append(type);

            segment = measurement.segment.id;
            if segment in computed_segments:
                self.measurements_per_segment[segment].append(measurement);
            else:
                self.measurements_per_segment[segment] = [measurement];
                computed_segments.append(segment);




    #### COMPUTE EXTRA VALUES ####

    def compute_directions(self):
        """
        Computes the directions for the measurements stored in the class instance. Direction refers
        to the direction of the car when the measurement was taken.
        """
        for measurement in self.measurements:
            segment_o: Segment = measurement.segment;
            position_b = segment_o.position_b;

            measurements_of_segment:List[Measurement] = self.measurements_per_segment[measurement.segment.id]
            first_measurement = measurements_of_segment[0];
            last_measurement = measurements_of_segment[len(measurements_of_segment) - 1];

            distance_m1 = self.calculate_distance(first_measurement.position, position_b);
            distance_m2 = self.calculate_distance(last_measurement.position, position_b);

            if distance_m2 < distance_m1:
                direction = 0;
            else:
                direction = 1;

            measurement.direction = direction;

        return;
    

    #### FINDING METHODS ####

    def get_measurement_by_id(self, measurement_id:str) -> Measurement:
        """
        Returns the Measurement that is identified by a certain id.

        :param measurement_id: Id of the measurement
        :type measurement_id: str
        """
        return self.measurements_per_id[measurement_id];

    def get_next_measurement_of_type_in_segment(self, type:str, created_at, segment_id:int) -> Measurement:
        measurements = self.measurements_per_segment[segment_id];
        for measurement in measurements:
            if measurement.type == type:
                if measurement.created_at >= created_at:
                    return measurement;
        return None;

    def get_previous_measurement_of_type_in_segment(self, type:str, created_at, segment_id:int) -> Measurement:
        measurements = self.measurements_per_segment[segment_id];
        for measurement in measurements:
            if measurement.type == type:
                if measurement.created_at <= created_at:
                    return measurement;
        return None;
    
    def get_closest_measurement_of_type_in_segment(self, type:str, created_at, segment_id:int):
        """
        Returns the measurement with the closest timestamp to another timestamp of a measurement
        of a certain type and a certain segment.

        :param type: Type of the measurement
        :type type: str
        :param created_at: Timestamp of reference
        :type created_at: str
        :param segment_id: Id of the segment of reference
        :type segment_id: int

        """
        res = self.get_previous_measurement_of_type_in_segment(type, created_at, segment_id)
        if res == None:
            res = self.get_next_measurement_of_type_in_segment(type, created_at, segment_id)
        return res;

    def get_measurements(self) -> List[Measurement]:
        """
        Returns all the measurements stored in the instance of the class.
        """
        
        return self.measurements;



    #### COMPUTE MEASUREMENTS METHODS ####


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


    # Determines if pointC is in the segment composed by points A and B
    def is_point_c_between_a_and_b(self, point_a, point_b, point_c):
        """
        Returns if a geographical point C is between a geographical point A and a 
        geographical point B.

        :param point_a: Latitude and longitude of point A.
        :type point_a: List[float]
        :param point_b: Latitude and longitude of point B.
        :type point_b: List[float]
        :param point_c: Latitude and longitude of point C.
        :type point_c: List[float]
        """
        distance_ab = geopy.distance.distance(point_a, point_b).km;
        distance_ac = geopy.distance.distance(point_a, point_c).km;
        distance_bc = geopy.distance.distance(point_b, point_c).km;

        sum = distance_ac + distance_bc;

        range_a = distance_ab - 0.005;
        range_b = distance_ab + 0.005;

        if (sum >= range_a) & (sum <= range_b):
            return True;       
        
        return False;

    # For a determined measurement looks for the segment it corresponds by calculating for all segments in it's way
    def compute_measurement(self, measurement:Measurement, segments:List[Segment]) -> int:
        """
        Assigns a segment to a measurement.

        :param measurement: Measurement that needs a segment to be assigned to it.
        :type measurement: Measurement
        :param segments: Candidate segments for assigning the measurement to.
        :type segments: List[Segment]
    
        """
        for segment in segments:
            point_a = (segment.position_a[0], segment.position_a[1]);
            point_b = (segment.position_b[0], segment.position_b[1]);
            point_c = (measurement.position[0], measurement.position[1]);
            if self.is_point_c_between_a_and_b(point_a, point_b, point_c):
                return segment.id;
        
        return -1;

    # {'distance_from_trace_point': 0.173, 'edge_index': 0,
    #  'type': 'matched', 'distance_along_edge': 0.931, 'lat': 55.70058, 'lon': 12.565007}
    # Assigns to each measurement a segment of it's way
    def compute_measurements(self, measurements: List[Measurement]) -> List[Measurement]:

        """
        Assigns segments to a list of measurements.
        
        :param measurements: Measurements that need a segment to be assigned to them.
        :type measurements: List[Measurement]
        """

        i = 0;
        res = [];

        segments_dictionary = Segments();
        for measurement in measurements:

            segment = self.compute_measurement(measurement, segments_dictionary.get_segments_in_a_way(measurement.way));
            
            if segment != -1:
                measurement.segment = Segments().get_segments_by_id(segment);
                res.append(measurement)
            i = i + 1;
            
        return res;

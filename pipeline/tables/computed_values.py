
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurement
from tables.segments import Segment
from auxiliar_modules.db_queries import get_computed_values_in_ways
from auxiliar_modules.db_queries import connect;
import row_types.computed_values as computed_values_classes;
import os;
import importlib;
import pkgutil;
from tables.computed_values_types import ComputedValuesTypes
from tables.measurements import Measurements
from tables.segments import Segments
from typing import Dict, List


class ComputedValue:
    """This is a conceptual class representation of a Computed Value.

    :param id: The id of the Computed Value.
    :type id: int
    :param measurement: Measurement from which the Computed Value was computed.
    :type measurement: Measurement
    :param segment: Segment to which the computed value pertains
    :type segment: Segment
    :param direction: Direction of the car related to the computed value
    :type direction: int
    :param type: The id of the type of computed value
    :type type: ComputedValueType
    :param value: The value of the Computed Value
    :type value: float
    """

    def __init__(self, id, measurement, type, value, segment, direction):
       
        if id == -1:
            self.id: int = id;
            self.measurement: Measurement = measurement;
            self.segment: Segment = measurement.segment;
            self.direction: int = measurement.direction;
            self.type: ComputedValueType = ComputedValuesTypes().get_type_by_name(self.TYPE);
            self.value: float = self.calculate_value(self.measurement);
        else:
            self.id: int = id;
            self.measurement: Measurement = measurement
            self.segment: Segment = segment;
            self.direction: int = direction;
            self.value: float = value;
            self.type: ComputedValueType = type;
        

    def get_db_row(self):
        """
        Returns a list of values to be inserted in the visualization database as a Computed Value
        """
        
        # return [self.Measurement.Id, self.Type, self.Value, self.Segment, self.Direction];
        return [self.measurement.id, self.type.id, self.value, self.segment.id, self.direction];

    def calculate_value(self, measurement):
        """Returns the value of the computed value. This method needs to be overridden by any subclass
        that implements a type of computed value.

        :param measurement: The measurement associated with the computed value
        :type measurement: Measurement
    
        """
        pass;

    @staticmethod
    def prerequisites(self, measurement):
        """
        Returns a boolean indicating if a computed value needs to be computed for a certain measurement.
        It is used as a filter to avoid computing all types of computed values for all measurements when it is not needed.

        :param measurement: Measurement associated with the Computed Value
        :type measurement: Measurement
        """
        pass;

def parse_computed_values(computed_values_rows:List) -> List[ComputedValue]:
    """Converts computed values rows retrieved from the database to ComputedValue objects to be used within the pipeline

        :param segment_rows: A list of lists of values representing ComputedValue objects
        :type segment_rows: List[]
    """
    res = [];
    for row in computed_values_rows:
        measurement = Measurements().get_measurement_by_id(row[1])
        segment = Segments().get_segments_by_id(row[4])
        type = ComputedValuesTypes().get_type_by_id(row[2])
        cv = ComputedValue(id = row[0], measurement=measurement, type=type, value=row[3], segment=segment, direction=row[5]);
        res.append(cv);
    return res;


class ComputedValues(object):
    """This is a conceptual class representation of the Computed Values table. It stores the computed values that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    
    computed_values: List[ComputedValue] = [];
    computed_values_to_insert: List[ComputedValue] = [];
    computed_values_in_db: List[ComputedValue] = [];

    computed_values_per_segment: Dict = {};

    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ComputedValues, cls).__new__(cls, *args, **kwargs)

        return cls._instance



    def compute_data(self, measurements, computed_ways):
        """Computes, creates and stores Computed Value objects in the class instance based on the ways and measurements passed as parameters.

        :param measurements: Measurements from which the function computes the ComputedValues
        :type measurements: List[Measurement]
        :param computed_ways: Ways whose Computed Values need to be retrieved from the database instead of computed.
        :type computed_ways: List[int]
        
        """
        self.computed_values_to_insert = self.generate_computed_values(measurements);
        self.computed_values_in_db = parse_computed_values(get_computed_values_in_ways(computed_ways));

        self.computed_values = self.computed_values_to_insert + self.computed_values_in_db;
        self.compute_dictionaries();
        pass;
    
    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.computed_values = [];
        self.computed_values_to_insert = [];
        self.computed_values_in_db = [];
        self.computed_values_per_segment = {};


    ### DATABASE ####

    def insert_into_db(self):
        """
        Inserts the Computed Values into the ComputedValues table in the visualization database. 
        """

        print("Inserting computed values in db")
        computedValues = self.computed_values_to_insert;
        if len(computedValues) == 0:
            return;
        rows = [];
        for computedValue in computedValues:
            if computedValue != None:
                rows.append(computedValue.get_db_row());

        cur, conn = connect();

        args_str = ','.join("('{MeasurementId}','{Type}',{Value}, {Segment}, {Direction})"
        .format(MeasurementId = x[0], Type = x[1], Value = x[2], Segment = x[3], Direction = x[4]) for x in rows)

        sql = """
            INSERT INTO "computed_values"("measurement", "type", "value", "segment", "direction") VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;

        
    ### COMPUTE VALUES METHODS ####

    def compute_dictionaries(self):
        """
        Computes dictionaries that store the computed values. The purpose of the dictionaries is for computed values to be
        accessed and searched in a most efficient way.
        """
        print("computing computed values dictionary")

        computed_values = self.computed_values;
        computed_segments = [];
        for value in computed_values:
            segment = value.segment.id;
            
            if segment not in computed_segments:
                self.computed_values_per_segment[segment] = [value];
                computed_segments.append(segment)
            else:
                self.computed_values_per_segment[segment].append(value)

    

    def get_computed_values_types_classes(self):
        """
        Returns the subclasses that implement the different computed values types in the pipeline.

        """
        res = [];
        path = os.path.dirname(computed_values_classes.__file__)
        list_modules = [name for _, name, _ in pkgutil.iter_modules([path])]

        for klassname in list_modules:
            module_path = 'row_types.computed_values.' + klassname;
            mod = importlib.import_module(module_path)
            klass = getattr(mod, klassname)
            res.append(klass);
        return res;



    def generate_computed_values(self, measurements: List[Measurement]) -> List[ComputedValue]:
        """
        Returns the computed values for a list of measurements.

        :param: measurements: Measurements from which to compute the Computed Values.
        :type measurements: List[Measurement]
      
        """
        computed_values = [];
        classes_types = self.get_computed_values_types_classes();
        print("Computing computed values")
        for measurement in measurements:
            for klass in classes_types:
                if klass.prerequisites(measurement):
                    computed_value = klass(-1, measurement, None, None, None, None);
                    
                    if computed_value.value != None:
                        computed_values.append(computed_value);

        return computed_values;



    def get_computed_values_in_segment(self, segment_id) -> List[ComputedValue]:
        """
        Returns the computed values in a segment.

        :param: segment_id: Id of the segment.
        :type segment_id: int
      
        """
        if segment_id in self.computed_values_per_segment:
            return self.computed_values_per_segment[segment_id];
        else: 
            return None;

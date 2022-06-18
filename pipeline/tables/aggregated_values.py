from tables.computed_values import ComputedValue
from tables.aggregation_methods import AggregationMethod
from tables.computed_values_types import ComputedValueType
from tables.segments import Segment
from tables.computed_values import ComputedValues
from tables.segments import Segments
from auxiliar_modules.db_queries import connect, get_computed_values_in_ways;
import row_types.aggregation_techniques as aggregation_techniques_classes;
import row_types.computed_values as computed_values_classes;
import os;
import importlib;
import pkgutil;
from tables.aggregation_methods import AggregationMethods
from tables.computed_values_types import ComputedValuesTypes
from typing import List

class AggregatedValue:
    """
    This is a conceptual class representation of an Aggregated Value.

    :param id: The id of the aggregated value.
    :type id: int
    :param segment: Segment to which the aggregated value pertains
    :type segment: Segment
    :param count: Number of values that have been used to compute the aggregated value
    :type count: int
    :param type: The type of the data that has been used to compute the aggregated value
    :type type: ComputedValueType
    :param aggregation: Aggregation method used to compute the aggregated value.
    :type aggregation: AggregationMethod
    :param direction: Direction of the car related to the aggregated value
    :type direction: int
    :param value: The value of the aggregated value
    :type value: float

    """

    def __init__(self, id, segment, count, type, aggregation,value, direction):
        self.id: int = id;
        self.segment: Segment = segment;
        self.count: int = count;
        self.type: ComputedValueType = type;
        self.aggregation: AggregationMethod = aggregation;
        self.value: float = value;
        self.direction: int = direction;
      

    def get_db_row(self):
        """
        Returns a list of values to be inserted in the visualization database as an Aggregated Value
        """
        return [self.segment.id, self.count, self.type.id, self.aggregation.id ,self.value, self.direction];



class AggregatedValues(object):
    """This is a conceptual class representation of the AggregatedValues table. It stores the aggregated values that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    aggregated_values_to_insert: List[AggregatedValue] = [];
    aggregated_values_to_update: List[AggregatedValue] = [];

    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(AggregatedValues, cls).__new__(cls, *args, **kwargs)
        return cls._instance



    def compute_data(self, computed_ways: List[int], not_computed_ways: List[int]):
        """Computes, creates and stores Aggregated Value objects in the class instance based on the ways passed as parameters.

        :param computed_ways: Ways whose aggregated values need to be updated in the database instead of inserted.
        :type computed_ways: List[int]
        :param not_computed_ways: Ways whose aggregated values need to be inserted in the database.
        :type not_computed_ways: List[int]
        
        """
        self.aggregated_values_to_insert = self.generate_aggregated_values(not_computed_ways);
        self.aggregated_values_to_update = self.generate_aggregated_values(computed_ways)
        pass;

    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.aggregated_values_to_insert = [];
        self.aggregated_values_to_update = [];

    def insert_into_db(self):
        """
        Inserts the aggregated values into the AggregatedValues table in the visualization database. 
        """
        data = self.aggregated_values_to_insert;
        if len(data) == 0:
            return;

        rows = [];
        for aggregatedValue in data:
            if aggregatedValue != None:
                rows.append(aggregatedValue.get_db_row());

        cur, conn = connect();

        args_str = ','.join("({Segment},{Count},'{Type}',{Value}, {Direction}, '{Aggregation}')"
        .format(Segment = x[0], Count = x[1], Type = x[2], Aggregation = x[3], Value = x[4], Direction=x[5]) for x in rows)

        sql = """
            INSERT INTO "aggregated_values"("segment", "count", "cv_type","value","direction","aggregation_method") VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;

    def update_values_in_db(self):
        """
        Updates the aggregated values into the AggregatedValues table in the visualization database. 
        """

        print("Updating values in db")
        aggregated_values = self.aggregated_values_to_update;

    
        if len(aggregated_values) == 0:
            return;

        rows = [];
        for value in aggregated_values:
            if value != None:
                rows.append(value.get_db_row());

        cur, conn = connect();
        sql = "";
        for row in rows:
            update = """UPDATE "aggregated_values" SET "count" = {count}
            , "value" = {value} WHERE "segment" = {segment} AND "cv_type" = {cv_type}
             AND "aggregation_method" = {aggregation_method}
             AND "direction" = {direction};""".format(
               count = row[1], segment=row[0], cv_type = row[2],value = row[4], aggregation_method=row[3], direction = row[5] 
            );
            sql = sql + update;
        cur.execute(sql);
        conn.commit();
        conn.close();
        return;

    def get_data_type_classes(self):
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

    def get_aggregation_type_classes(self):
        """
        Returns the subclasses that implement the different aggregation methods in the pipeline.

        """
        res = [];
        path = os.path.dirname(aggregation_techniques_classes.__file__)
        list_modules = [name for _, name, _ in pkgutil.iter_modules([path])]

        for klassname in list_modules:
            module_path = 'row_types.aggregation_techniques.' + klassname;
            mod = importlib.import_module(module_path)
            klass = getattr(mod, klassname)
            res.append(klass);
        return res;


    def get_values_of_type_and_direction(self, computed_values:List[ComputedValue], type_id: int, direction: int) -> List[int]:
        res = [];
        for value in computed_values:
            if value.type.id == type_id and (value.direction == direction or direction == 2):
                res.append(value);
        return res;

    
    def generate_aggregated_values(self, ways: List[int]) -> List[AggregatedValue]:
        """
        Returns the aggregated values for a list of ways.

        :param: ways: Ways to compute the aggregated values for.
        :type ways: List[int]
      
        """
        aggregated_values = [];
        classesTypes = self.get_data_type_classes();

        print("Computing aggregated values")

        aggregation_methods = AggregationMethods().get_aggregation_methods();
        for way in ways:
            segments = Segments().get_segments_in_a_way(way)

            for segment in segments:
                computed_values = ComputedValues().get_computed_values_in_segment(segment.id);
                if computed_values == None:
                    continue;

                for dataType in classesTypes:
                    
                    computed_value_type_id = ComputedValuesTypes().get_id_of_name(dataType.TYPE);
                    aggregations = dataType.AGGREGATIONS;
                    directions = [0, 1, 2];
                    for direction in directions:

                        values = self.get_values_of_type_and_direction(computed_values, computed_value_type_id, direction);
                        values = list(map(lambda value: value.value, values))

                        for method in aggregation_methods:
                            if len(aggregations) == 0 or method.NAME in aggregations:
                                aggregatedData = method.calculate_value(values);

                                if aggregatedData != None:
                                    cv_type = ComputedValuesTypes().get_type_by_id(computed_value_type_id)
                                    aggregation_method = AggregationMethods().get_method_by_name(method.NAME);
                                    av = AggregatedValue(-1, segment, len(values), cv_type, aggregation_method, aggregatedData, direction);
                                    aggregated_values.append(av);

        return aggregated_values;

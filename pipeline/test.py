import unittest
import csv;
import time;
from auxiliar_modules.auxiliar_classes import NodesDictionary, WaysDictionary
from auxiliar_modules.map_matching import format_and_map_match_measurements;
from tables.measurements import Measurements;
from tables.segments import Segments;
from tables.segments_properties import SegmentsProperties;
from tables.computed_values import ComputedValues;
from tables.aggregated_values import AggregatedValues;
from tables.computed_values_types import ComputedValuesTypes;
from tables.aggregation_methods import AggregationMethods;
from main import *;

class TestTables(unittest.TestCase):

    computed_ways = [];
    not_computed_ways = [];
    measurements = [];


    def setUp(self):
        self.startTime = time.time()

    def tearDown(self):
        t = time.time() - self.startTime
        print('%s: %.3f' % (self.id(), t))

    def test_1_pre_processing(self):
        measurements_from_db = [];

        file = open("./test_data/trip-2857262b-71db-49df-8db6-a042987bf0eb.csv")
        csvreader = csv.reader(file)
        for row in csvreader:
            measurements_from_db.append(row)
        file.close()

        measurements, nodes, ways = format_and_map_match_measurements(measurements_from_db)

        
        NodesDictionary().fill(nodes);
        WaysDictionary().fill(ways);
       
        self.__class__.computed_ways, self.__class__.not_computed_ways = classify_ways(WaysDictionary().get_ways_ids());

        self.__class__.not_computed_ways = self.__class__.not_computed_ways + self.__class__.computed_ways; 
        self.__class__.computed_ways = [];
        self.__class__.measurements = measurements;

    def test_2_computed_values_types_table(self):
        ComputedValuesTypes().compute_data();

    def test_3_aggregation_methods_table(self):
        AggregationMethods().compute_data();

    def test_4_segments_table(self):

        Segments().compute_data(self.__class__.computed_ways, self.__class__.not_computed_ways);
        
    
    def test_5_measurements_table(self):
        print("Total amount of measurements is: " + str(len(self.__class__.measurements)))
        Measurements().compute_data(self.__class__.measurements);

    def test_6_segments_properties_table(self):
        SegmentsProperties().compute_data(self.__class__.computed_ways, self.__class__.not_computed_ways);

    def test_7_computed_values_table(self):
        ComputedValues().compute_data(Measurements().get_measurements(), self.__class__.computed_ways)

    def test_8_aggregated_values_table(self):
        AggregatedValues().compute_data(self.__class__.computed_ways, self.__class__.not_computed_ways);


if __name__ == '__main__':
    unittest.main()
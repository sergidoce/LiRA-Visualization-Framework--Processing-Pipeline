from auxiliar_modules.auxiliar_classes import NodesDictionary, WaysDictionary
from tables.measurements import Measurements, format_for_map_matching, format_measurements, filter_measurements;
from tables.segments import Segments;
from tables.segments_properties import SegmentsProperties;
from tables.computed_values import ComputedValues;
from tables.aggregated_values import AggregatedValues;
from tables.computed_values_types import ComputedValuesTypes;
from tables.aggregation_methods import AggregationMethods;
from auxiliar_modules.map_matching import format_and_map_match_measurements;
from auxiliar_modules.db_queries import get_computed_ways, get_raw_measurements_from_main_db, delete_all;

def insert_data_into_db():
    AggregationMethods().insert_into_db();
    ComputedValuesTypes().insert_into_db();
    Segments().insert_into_db();
    Measurements().insert_into_db();
    SegmentsProperties().insert_into_db();
    ComputedValues().insert_into_db();  
    AggregatedValues().insert_into_db();
    AggregatedValues().update_values_in_db();

def drop_data():
    AggregationMethods().drop();
    ComputedValuesTypes().drop();
    Segments().drop();
    Measurements().drop();
    SegmentsProperties().drop();
    ComputedValues().drop();  
    AggregatedValues().drop();

    WaysDictionary().drop();
    NodesDictionary().drop();


def classify_ways(way_ids):
    
    all_computed_ways = get_computed_ways();
    computed_ways = []
    for way in all_computed_ways:
        way_id = way[0];
        if way_id in way_ids:
            computed_ways.append(way_id)

    not_computed_ways = [];
    for id in way_ids:
    
        if id not in computed_ways:
            not_computed_ways.append(id);

    return computed_ways, not_computed_ways;




def main(trip: str):
    


    # STEP 1 AND 2
    print("Performing Step 1 and 2 - Fetch and Format and Map Match Car Measurements");
    measurements_from_db = get_raw_measurements_from_main_db(trip);
    measurements, nodes, ways = format_and_map_match_measurements(measurements_from_db);

    NodesDictionary().fill(nodes);
    WaysDictionary().fill(ways);

    # STEP 3
    print("Performing Step 3 - Divide Map Matched Ways into Computed and Not Computed Ways")
    computed_ways, not_computed_ways = classify_ways(WaysDictionary().get_ways_ids());

    ComputedValuesTypes().compute_data();
    AggregationMethods().compute_data();

    # STEP 4
    print("Performing Step 4 - Divide the Not Computed Ways into Segments")
    Segments().compute_data(computed_ways, not_computed_ways);

    # STEP 5
    print("Performing Step 5 - Assign Each Measurement to a Segment")
    Measurements().compute_data(measurements);

    # STEP 6
    print("Performing Step 6 - Compute Segments Properties")
    SegmentsProperties().compute_data(computed_ways, not_computed_ways);

    # STEP 7
    print("Performing Step 7 - Compute Measurements")
    ComputedValues().compute_data(Measurements().get_measurements(), computed_ways)

    # STEP 8
    print("Performing Step 8 - Compute Aggregated Values")
    AggregatedValues().compute_data(computed_ways, not_computed_ways);

    # STEP 9
    print("Performing Step 9 - Insert Data")
    insert_data_into_db();
    drop_data();
    
    


if __name__ == "__main__":

    #delete_all();

    acceleration_engine_rpm_trip = 'd33505d1-f328-4a7a-be38-049d392d1cb5' # 4974
    acceleration_trip = '9d056547-7872-4ae4-8fbe-3c61214c4c00' # 4975
    energy_and_speed = '2857262b-71db-49df-8db6-a042987bf0eb'; #8064

    trips = [acceleration_engine_rpm_trip, acceleration_trip, energy_and_speed]
    for trip in trips:
        main(trip);

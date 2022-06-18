from tables.computed_values import ComputedValue;
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurements, Measurement;
from tables.segments_properties import SegmentsProperties
import math;

class HillClimbingForce(ComputedValue):


    TYPE = "Hill Climbing Force";
    DESCRIPTION = "Hill Climbing Force component of the Traction Force of the energy measurement";
    UNITS = "Newtons";
    AGGREGATIONS = [];

    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
     
    
    
    def calculate_value(self, measurement: Measurement):

        inclination_of_segment = SegmentsProperties().get_segment_property("Inclination", measurement.segment.id)

        if inclination_of_segment != None:

            if(measurement.direction == 1):
                inclination_of_segment = inclination_of_segment * -1;

            return (1966 + 80) * math.sin(inclination_of_segment) * 9.81;

        return None;
    
    @staticmethod
    def prerequisites(measurement: Measurement):
        if measurement.type != 'obd.trac_cons':
            return False;
        else:
            return True;

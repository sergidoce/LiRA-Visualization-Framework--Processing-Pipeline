from tables.computed_values import ComputedValue;
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurements, Measurement;
from tables.segments import Segments;

class Speed(ComputedValue):


    TYPE = "Speed";
    DESCRIPTION = "Speed of the vehicle";
    UNITS = "km/h";
    AGGREGATIONS = [];

    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
     
    
    
    def calculate_value(self, measurement: Measurement):
        
        return measurement.value;
    
    @staticmethod
    def prerequisites(measurement: Measurement):
        if measurement.type != 'obd.spd_veh':
            return False;
        else:
            return True;

from tables.computed_values import ComputedValue;
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurements, Measurement;
from tables.segments import Segments;

class RevolutionsPerMinute(ComputedValue):


    TYPE = "Revolutions per minute";
    DESCRIPTION = "How many times the crankshaft of the engine makes one full rotation in a minute";
    UNITS = "rpm";
    AGGREGATIONS = [];

    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
     
    
    
    def calculate_value(self, measurement: Measurement):
        
        return measurement.value;
    
    @staticmethod
    def prerequisites(measurement: Measurement):
        if measurement.type != 'obd.rpm':
            return False;
        else:
            return True;

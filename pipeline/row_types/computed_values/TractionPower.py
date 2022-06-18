from tables.computed_values import ComputedValue;
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurements, Measurement;
from tables.segments import Segments;

class TractionPower(ComputedValue):


    TYPE = "Traction power";
    DESCRIPTION = "Traction power taken from the vehicle";
    UNITS = "W";
    AGGREGATIONS = [];

    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
     
    
    
    def calculate_value(self, measurement: Measurement):
        
        return measurement.value;
    
    @staticmethod
    def prerequisites(measurement: Measurement):
        if measurement.type != 'obd.trac_cons':
            return False;
        else:
            return True;
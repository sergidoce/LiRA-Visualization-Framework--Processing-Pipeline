from tables.computed_values import ComputedValue;
from tables.computed_values_types import ComputedValueType
from tables.measurements import Measurements, Measurement;
from tables.segments import Segments;

class InertialForce(ComputedValue):


    TYPE = "Inertial Force";
    DESCRIPTION = "Inertial Force component of the Traction Force of the energy measurement";
    UNITS = "Newtons";
    AGGREGATIONS = [];

    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
     
    
    
    def calculate_value(self, measurement: Measurement):

        segment = measurement.segment;
        closest_acceleration_measurement = Measurements().get_closest_measurement_of_type_in_segment('acc.xyz.x', measurement.created_at, segment.id);

        if closest_acceleration_measurement != None:
            acceleration = closest_acceleration_measurement.value;
            return 0.05 * (1966 + 80) * acceleration;
            
        return None;
    
    @staticmethod
    def prerequisites(measurement: Measurement):
        if measurement.type != 'obd.trac_cons':
            return False;
        else:
            return True;

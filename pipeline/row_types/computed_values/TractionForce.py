from fileinput import close
from tables.computed_values_types import ComputedValueType
from tables.computed_values import ComputedValue;
from tables.measurements import Measurements, Measurement;
from tables.segments import Segments;

class TractionForce(ComputedValue):

    TYPE = "Traction Force";
    DESCRIPTION = "Traction Force calculated from Traction power";
    UNITS = "Newtons";
    AGGREGATIONS = [];


    def __init__(self, id: int, measurement: Measurement, type: ComputedValueType, value: float, segment: int, direction: int):
        super().__init__(id, measurement, type, value, segment, direction);
        
    

    def calculate_value(self, measurement: Measurement):

        measurements_table = Measurements(); 
        segment = measurement.segment;
        power = measurement.value;
        # We substract 160 from erroneous measurement by default
        power = power - 160;
        # We convert it to W
        power = power * 1000;

        closest_velocity_measurement = measurements_table.get_closest_measurement_of_type_in_segment('obd.spd_veh', measurement.created_at, segment.id);
        if closest_velocity_measurement != None:
            velocity = closest_velocity_measurement.value;

            if velocity == 0:
                return 0;
            else:
                return power / velocity;

        return None;

    @staticmethod
    def prerequisites(measurement: Measurement):

        # We only calculate traction force for energy consumption measurements
        if measurement.type != 'obd.trac_cons':
            return False;
        else:
            return True;
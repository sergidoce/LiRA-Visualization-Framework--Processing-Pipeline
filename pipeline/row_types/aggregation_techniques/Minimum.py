from tables.aggregation_methods import AggregationMethod;
from typing import List

class Minimum(AggregationMethod):

    NAME = "Minimum";
    DESCRIPTION = "Minimum value";


    def __init__(self, id: int) -> None:
        super().__init__(id);


    def calculate_value(self, values: List[float]):

        if len(values) == 0:
            return None;
            
        return min(values);

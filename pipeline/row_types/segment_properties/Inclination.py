import math;
from tables.segments_properties import SegmentProperty
from tables.segments import Segment
from typing import List
import os;
import requests;

class Inclination(SegmentProperty):

    def __init__(self, id: int, segment: Segment):
        super().__init__(id, segment)
        self.type = 'Inclination';

    def calculate_value(self, segment: Segment):

        elevation_point_a = self.get_elevation(segment.position_a);
        elevation_point_b = self.get_elevation(segment.position_b);

        a = elevation_point_b - elevation_point_a;
        b = segment.length * 1000;
        c = math.sqrt((a * a) + (b * b));
        
        return math.asin(a/c);
    

    def get_elevation(self, point: List[float]) -> float:
        
        url = 'https://services.datafordeler.dk/DHMTerraen/DHMKoter/1.0.0/GEOREST/HentKoter?format=json&username={id}&password={pwd}&geop=POINT({lat} {lon})&georef=EPSG:4326'.format(
            id=os.getenv('KOTER_USER'), 
            pwd = os.getenv('KOTER_PWD'),
            lat = point[0],
            lon= point[1]);
        response = requests.get(url);
        resJSON = response.json();
        return resJSON['HentKoterRespons']['data'][0]['kote']
    

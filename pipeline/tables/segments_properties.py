from auxiliar_modules.db_queries import connect, get_segment_properties_in_ways;
from tables.segments import Segments, Segment;
import os;
import pkgutil;
import row_types.segment_properties as segment_properties_classes;
import importlib;
from typing import List

class SegmentProperty:
    """This is a conceptual class representation of a Segment Property.

    :param id: The id of the Segment Property.
    :type id: int
    :param segment: Segment associated with the Segment Property
    :type segment: Segment
    :param type: Type of segment property
    :type type: str
    :param value: Value of the segment property. 
    :type value: float
    
    """
    def __init__(self, id, segment, type = None, value = None):
        self.id = id;
        if id == -1:
            self.segment: Segment = segment;
            self.type: str = None;
            self.value: float = self.calculate_value(self.segment);
        else:
            self.segment: Segment = segment;
            self.type: str = type;
            self.value: float = value;


    def calculate_value(self, segment):
        """Returns the value for the segment property. This method needs to be overridden by any subclass
        that implements a segment property.

        :param segment: The segment associated with the segment property
        :type segment: Segment
    
        """
        pass;

    def get_db_row(self):
        """Returns a list of values to be inserted in the visualization database as a Segment Property
        """
        return [self.segment.id, self.type, self.value];


def parse_segment_properties(segment_properties_rows: List) -> List[SegmentProperty]:
    """Converts segment property rows retrieved from the database to segment property objects to be used within the pipeline

        :param segment_rows: A list of lists of values representing Segment Properties  
        :type segment_rows: List[SegmentProperty]
    """
    res = []
    for row in segment_properties_rows:
        segment = Segments().get_segments_by_id(row[1])
        segment_property = SegmentProperty(row[0], segment, row[2], row[3])
        res.append(segment_property);
    return res;


class SegmentsProperties(object):
    """This is a conceptual class representation of the SegmentsProperties table. It stores the segment properties that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    segment_properties: List[SegmentProperty] = [];
    segment_properties_to_insert: List[SegmentProperty] = [];
    segment_properties_in_db: List[SegmentProperty] = [];

    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SegmentsProperties, cls).__new__(cls, *args, **kwargs)
        return cls._instance



    def compute_data(self, computed_ways: List[int], not_computed_ways: List[int]):
        """Computes, creates and stores Segment Property objects in the class instance based on the ways passed as parameters.

        :param computed_ways: Ways whose segments properties need to be retrieved from the database instead of computed.
        :type computed_ways: List[int]
        :param not_computed_ways: Ways that have never been computed and whose segment properties need to be inserted in the visualization database.
        :type not_computed_ways: List[int]
        """
        self.segment_properties_to_insert = self.generate_segments_properties(not_computed_ways);
        self.segment_properties_in_db = parse_segment_properties(get_segment_properties_in_ways(computed_ways));

        self.segment_properties = self.segment_properties_to_insert + self.segment_properties_in_db;
        pass;


    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.segment_properties = [];
        self.segment_properties_in_db = [];
        self.segment_properties_to_insert = [];

    ### DATABASE ####

    def insert_into_db(self):
        """
        Inserts the Segment Properties into the SegmentProperties table in the visualization database. 
        """
        print("Inserting segment properties in db")

        segmentproperties = self.segment_properties_to_insert;
        if len(segmentproperties) == 0:
            return;
        rows = [];
        for segmentProperty in segmentproperties:
            if segmentProperty != None:
                rows.append(segmentProperty.get_db_row()); 

        cur, conn = connect();

        args_str = ','.join("('{Segment}','{Type}',{Value})"
        .format(Segment = x[0], Type = x[1], Value = x[2]) for x in rows)

        sql = """
            INSERT INTO "segment_properties"("segment", "type", "value") VALUES
            """

        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;


        

    #### SEGMENTS PROPERTIES GENERATION ####

    def get_segments_properties_types_classes(self):
        """
        Returns the subclasses that implement the different segment properties in the pipeline.

        """
        res = [];
        path = os.path.dirname(segment_properties_classes.__file__)
        list_modules = [name for _, name, _ in pkgutil.iter_modules([path])]

        for klassname in list_modules:
            module_path = 'row_types.segment_properties.' + klassname;
            mod = importlib.import_module(module_path)
            klass = getattr(mod, klassname)
            res.append(klass);
        return res;

    def generate_segments_properties(self, ways_to_compute: List[int]) -> List[SegmentProperty]:
        """
        Returns the segment properties for the segments in the ways passed as parameter.

        :param ways_to_compute: Ids of the ways that need their segment properties need to be computed
        :type ways_to_compute: List[int]
        """
        res = [];

        types_classes = self.get_segments_properties_types_classes();
        segments_table = Segments();

        print("Computing segments properties")
        for way in ways_to_compute:
            segments = segments_table.get_segments_in_a_way(way);

            for segment in segments:
                for klass in types_classes:
                    property = klass(-1, segment);

                    if property.value != None:
                        res.append(property);
            
        return res;


    def get_segment_property(self, property_name: str, segment_id: int) -> float:
        """
        Returns a segment property of a segment

        :param property_name: Name of the segment property
        :type property_name: str
        :param segment_id: Id of the segment 
        :type segment_id: int
        """
        for segment_property in self.segment_properties:
            if segment_property.type == property_name and segment_property.segment.id == segment_id:
                return segment_property.value;
        return None;
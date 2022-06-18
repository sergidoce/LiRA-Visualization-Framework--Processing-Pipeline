from typing import List, Dict

class Node:
    """This is a conceptual class representation of a Node.

    :param id: The id of the node.
    :type id: int
    :param lat: Latitude component of the geographical point in which the Node is located.
    :type lat: float
    :param lon: Longitude component of the geographical point in which the Node is located.
    :type lon: float
    """
    def __init__(self, id: int, lat: float, lon: float):
        self.id = id;
        self.lat = lat;
        self.lon = lon;


class Way:
    """This is a conceptual class representation of a Node.

    :param id: The id of the way.
    :type id: int
    :param nodes: List of nodes contained in the way.
    :type nodes: List[Node]
    """
    def __init__(self, id: int, nodes: List[Node]):
        self.id = id;
        self.nodes = nodes;



class WaysDictionary:
    """This is a conceptual class representation of a dictionary of ways. It is used to store all the ways
    in the pipeline into a single instance of a Singleton class. It ensures the efficient access to the data
    by using dedicated methods and data structures.
    """

    ways_dict: Dict = {};

     #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(WaysDictionary, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def fill(self, ways: List[Way]):
        """
        Fills the WayDictionary with the ways passed as parameter.

        :param ways: List of ways that need to be stored
        :type ways: List[Way]

        """
        for way in ways:
            print(way.id)
            self.ways_dict[way.id] = way;

        return;

    def drop(self):
        """
        Deletes all the data in the WayDictionary instance.

        """
        self.ways_dict = {};


    def get_way_by_id(self, way_id: int) -> Way:
        """
        Returns the Way that has the id passed as parameter.

        :param way_id: Id of the Way
        :type way_id: int
        """
        return self.ways_dict[way_id];
    
    def get_ways_ids(self) -> List[int]:
        """
        Returns all the ids of the ways stored in the dictionary.

        """
        return self.ways_dict.keys();


class NodesDictionary:
    """This is a conceptual class representation of a dictionary of nodes. It is used to store all the nodes
    in the pipeline into a single instance of a Singleton class. It ensures the efficient access to the data
    by using dedicated methods and data structures.
    """

    nodes_dict: Dict = {};

     #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(NodesDictionary, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def fill(self, nodes: List[Node]):
        """
        Fills the NodeDictionary with the nodes passed as parameter.

        :param nodes: List of nodes that need to be stored
        :type nodes: List[Node]

        """
        for node in nodes:
            self.nodes_dict[node.id] = node;

    def drop(self):
        """
        Deletes all the data in the NodeDictionary instance.

        """
        self.nodes_dict = {};


    def get_node_by_id(self, node_id: int) -> Node:
        """
        Returns the Node that has the id passed as parameter.

        :param node_id: Id of the Node
        :type node_id: int
        """
        return self.nodes_dict[node_id];
    




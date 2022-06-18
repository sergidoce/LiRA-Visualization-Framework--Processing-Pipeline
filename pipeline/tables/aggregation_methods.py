from auxiliar_modules.db_queries import connect;
import row_types.aggregation_techniques as aggregation_methods_classes;
import os;
import importlib;
import pkgutil;
from typing import List

class AggregationMethod:
    """This is a conceptual class representation of an Aggregation Method.

    :param id: The id of the Aggregation Method
    :type id: int
    :param name: Name of the Aggregation Method
    :type name: str
    :param description: Description of the Aggregation Method
    :type description: str
    """
    def __init__(self, id):
        self.id: int = id;
        self.name: str = self.NAME;
        self.description:str = self.DESCRIPTION;

    def calculate_value(self, values):
        """Returns the result of applying the aggregation method to a set of values. This method needs to be overridden by any subclass
        that implements a type of computed value.

        :param values: List of values to aggregate.
        :type values: List[float]
    
        """
        pass;  

    def get_db_row(self):
        """
        Returns a list of values to be inserted in the visualization database as an Aggregation Method
        """
        return [self.id, self.NAME, self.DESCRIPTION];
  


class AggregationMethods(object):
    """This is a conceptual class representation of the SegmentsProperties table. It stores the segment properties that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    
    computed_aggregation_methods: List[AggregationMethod] = [];


    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(AggregationMethods, cls).__new__(cls, *args, **kwargs)

        return cls._instance


    

    def compute_data(self):
        """
        Computes, creates and stores Aggregation Methods objects in the class instance based on the subclasses implemented in the pipeline as AggregationMethod classes.
        """
   
        self.computed_aggregation_methods = self.generate_aggregation_methods();
        pass;

    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.computed_aggregation_methods = [];

    ### DATABASE ####

    def get_inserted(self) -> List[int]:
        """
        Returns the ids of the aggregation methods that are already in the visualization database.
        """
        cur, conn = connect();

        sql = """
            SELECT "id" FROM "aggregation_methods"
            """
        cur.execute(sql);
        rows = cur.fetchall();
        conn.commit();
        conn.close();

        ids = [x[0] for x in rows]

        return ids;


    def insert_into_db(self):
        """
        Inserts the Aggregation Methods into the AggregationMethods table in the visualization database. 
        """

        methods = self.computed_aggregation_methods;

        already_inserted = self.get_inserted();

        rows = [];
        for method in methods:
            if method != None and method.id not in already_inserted:
                rows.append(method.get_db_row());

        if len(rows) == 0:
            return;

        cur, conn = connect();

        args_str = ','.join("('{id}','{name}','{description}')"
        .format(id = x[0], name = x[1], description = x[2]) for x in rows)

        sql = """
            INSERT INTO "aggregation_methods"("id", "name", "description") VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;

        
    ### COMPUTE VALUES METHODS ####


    def get_aggregation_methods_classes(self):
        """
        Returns the AggregationMethod subclasses that implement the different aggregation methods in the pipeline.

        """
        res = [];
        path = os.path.dirname(aggregation_methods_classes.__file__)
        list_modules = [name for _, name, _ in pkgutil.iter_modules([path])]

        for klassname in list_modules:
            module_path = 'row_types.aggregation_techniques.' + klassname;
            mod = importlib.import_module(module_path)
            klass = getattr(mod, klassname)
            res.append(klass);
        return res, list_modules;

    def generate_ids(self, list_modules) -> List[int]:
        res = [];
        for name in list_modules:
            n = [ord(x) - 96 for x in name]
            res.append(sum(n));

        return res;

    def generate_aggregation_methods(self) -> List[AggregationMethod]:
        """
        Returns the Aggregation Methods implemented in the pipeline.
      
        """
        aggregation_methods = [];
        classes_types, list_modules = self.get_aggregation_methods_classes();

        ids = self.generate_ids(list_modules)

        i = 0;
        print("Computing aggregation methods")
        for klass in classes_types:
            aggregation_method = klass(ids[i])
            aggregation_methods.append(aggregation_method);
            i = i +1;
        return aggregation_methods;


    def get_aggregation_methods(self) -> List[AggregationMethod]:
        """
        Returns all AggregationMethods in the pipeline
      
        """
        return self.computed_aggregation_methods;

    def get_method_by_name(self, name:str) -> AggregationMethod:
        """
        Returns the AggregationMethod with the name equal to the name passed as parameter.
      
        :param name: Name of the Aggregation Method
        :type name: str
        """
        for method in self.computed_aggregation_methods:
            if method.name == name:
                return method;
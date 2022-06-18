from auxiliar_modules.db_queries import connect;
import row_types.computed_values as computed_values_classes;
import os;
import importlib;
import pkgutil;
from typing import List


class ComputedValueType:
    """This is a conceptual class representation of a Computed Value Type.

    :param id: The id of the Computed Value Type.
    :type id: int
    :param name: Name of the Computed Value Type
    :type name: str
    :param description: Description of the Computed Value Type
    :type description: str
    :param units: Describes the units of the Computed Value
    :type units: str
    """
    def __init__(self, id, name, description, units):
        self.id: int = id;
        self.name: str = name;
        self.description: str = description;
        self.units: str = units;
        

    def get_db_row(self):
        """
        Returns a list of values to be inserted in the visualization database as a Computed Value Type
        """
        return [self.id, self.name, self.description, self.units];


class ComputedValuesTypes(object):
    """This is a conceptual class representation of the ComputedValuesTypes table. It stores the Computed Values Types that
    are used during data processing in the pipeline and allows the access to them in an efficient way.
    It is programmed as a Singleton so one instance exists at the same time and so it can be accessed from any point
    in the pipeline.
    """
    computed_values_types: List[ComputedValueType] = [];


    #### SINGLETON ####

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ComputedValuesTypes, cls).__new__(cls, *args, **kwargs)
            
        return cls._instance



    def compute_data(self):
        """
        Computes, creates and stores Computed Value Type objects in the class instance based on the subclasses implemented in the pipeline as ComputedValue classes.
        """
        self.computed_values_types = self.generate_computed_values_types();
        pass;

    def drop(self):
        """
        Deletes all the data stored in the class instance.
        """
        self.computed_values_types = [];


    ### DATABASE ####

    def get_inserted(self) -> List[int]:
        """
        Returns the ids of the computed value types that are already in the visualization database.
        """
        cur, conn = connect();

        sql = """
            SELECT "id" FROM "computed_values_types"
            """
        cur.execute(sql);
        rows = cur.fetchall();
        conn.commit();
        conn.close();

        ids = [x[0] for x in rows]

        return ids;



    def insert_into_db(self):
        """
        Inserts the Computed Value Types into the ComputedValueTypes table in the visualization database. 
        """

        types = self.computed_values_types;

        already_inserted = self.get_inserted();
    

        rows = [];
        for computed_value_type in types:
            if computed_value_type != None and computed_value_type.id not in already_inserted:
                rows.append(computed_value_type.get_db_row());

        if len(rows) == 0:
            return;

        cur, conn = connect();

        args_str = ','.join("('{id}','{name}','{description}', '{units}')"
        .format(id = x[0], name = x[1], description = x[2], units = x[3]) for x in rows)

        sql = """
            INSERT INTO "computed_values_types"("id", "name", "description", "units") VALUES
            """
        cur.execute(sql + " " + args_str);

        conn.commit();
        conn.close();
        return;

        
    ### COMPUTE VALUES METHODS ####


    def get_computed_values_types_classes(self):
        """
        Returns the ComputedValue subclasses that implement the different computed values types in the pipeline.

        """
        res = [];
        path = os.path.dirname(computed_values_classes.__file__)
        list_modules = [name for _, name, _ in pkgutil.iter_modules([path])]

        for klassname in list_modules:
            module_path = 'row_types.computed_values.' + klassname;
            mod = importlib.import_module(module_path)
            klass = getattr(mod, klassname)
            res.append(klass);
        return res, list_modules;



    def generate_ids(self, list_modules: List[str]) -> List[int]:
        """
        Returns a list of unique ids for a list of Computed Value Types.

        :param list_modules: Names of the modules that contain the implementations of the ComputedValue subclasses.
        :type list_modules: List[str]
        """
        res = [];
        for name in list_modules:
            n = [ord(x) - 96 for x in name]
            res.append(sum(n));

        return res;

    def generate_computed_values_types(self) -> List[ComputedValueType]:
        """
        Returns the Computed Value Types implemented in the pipeline.
      
        """
        computed_values_types = [];
        classes_types, list_modules = self.get_computed_values_types_classes();


        ids = self.generate_ids(list_modules);
        i = 0;
        print("Computing computed values types")
        for klass in classes_types:
            cv_type = ComputedValueType(ids[i], klass.TYPE, klass.DESCRIPTION, klass.UNITS)
            computed_values_types.append(cv_type);
            i = i +1;
        return computed_values_types;



    def get_id_of_name(self, name:str) -> int:
        """
        Returns id of a Computed Value Type with the name equal to the name passed as a parameter.

        :param: name: Name of the Computed Value Type
        :type name: str
      
        """
        for type in self.computed_values_types:
            if type.name == name:
                return type.id;


    def get_type_by_id(self, id:int) -> ComputedValueType:
        """
        Returns a Computed Value Type with the id equal to the id passed as a parameter.

        :param: id: Id of the Computed Value Type
        :type id: int
      
        """
        for cv_type in self.computed_values_types:
            if int(cv_type.id) == int(id):
                return cv_type;

    def get_type_by_name(self, name:str) -> ComputedValueType:
        """
        Returns a Computed Value Type with the name equal to the name passed as a parameter.

        :param: name: Name of the Computed Value Type
        :type name: str
      
        """
        for cv_type in self.computed_values_types:
            if str(cv_type.name) == str(name):
                return cv_type;
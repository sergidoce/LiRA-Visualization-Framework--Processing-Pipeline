import psycopg2 as db;
from dotenv import load_dotenv
import os


#<-------------- CONNECTION METHODS ------------->



def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    load_dotenv();
    try:
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = db.connect(host="liravisualization.postgres.database.azure.com",
                database="postgres",
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
        );
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        return cur, conn;
    except (Exception, db.DatabaseError) as error:
        print(error)


def connect_to_main_db():
    """ Connect to the PostgreSQL database server """
    conn = None
    load_dotenv();
    try:
        
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = db.connect(host="liradb.compute.dtu.dk",
                database="postgres",
                port='5435',
                user=os.getenv('DB_USER_MAIN'),
                password=os.getenv('DB_PASSWORD_MAIN')
        );
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        return cur, conn;
    except (Exception, db.DatabaseError) as error:
        print(error)



def get_raw_measurements_from_main_db(trip):
    cur, conn = connect_to_main_db();
    sql = """
        SELECT * FROM "Measurements" WHERE "FK_Trip"='{}' AND "T" IN ('obd.trac_cons', 'acc.xyz', 'obd.spd_veh', 'obd.rpm')
        ORDER BY "Created_Date" ASC
        """.format(trip);
    cur.execute(sql);
    rows = cur.fetchall();
    conn.close();
    return rows;


def get_segments_from_ways(ways):

    if len(ways) == 0:
        return [];

    cur, conn = connect();

    strings = [str(x) for x in ways]
    ways_string = ','.join(strings);
    ways_string = '(' + ways_string + ')';

    sql = """
        SELECT *, ST_X("position_a"), ST_Y("position_a"), ST_X("position_b"), ST_Y("position_b") FROM "segments" WHERE "way" IN {}
        """.format(ways_string);
    cur.execute(sql);
    rows = cur.fetchall();
    conn.close();
    return rows;


def get_computed_ways():
    cur, conn = connect();
    sql = """
        SELECT DISTINCT "way" FROM "segments"
        """
    cur.execute(sql);
    rows = cur.fetchall();
    conn.close();
    return rows;

def get_computed_values_in_ways(ways):

    if len(ways) == 0:
        return [];

    cur, conn = connect();

    strings = [str(x) for x in ways]
    ways_string = ','.join(strings);
    ways_string = '(' + ways_string + ')';

    sql = """
        SELECT "computed_values"."id", "computed_values"."measurement", "computed_values"."type", "computed_values"."value", "measurements"."segment", "measurements"."direction"
        FROM "measurements" INNER JOIN "segments"
        ON "segments"."id" = "measurements"."segment"
        INNER JOIN "computed_values"
        ON "computed_values"."measurement" = "measurements"."id"
        WHERE "segments"."way" IN {ways_string}
        """.format(ways_string=ways_string);
    cur.execute(sql);
    rows = cur.fetchall();
    conn.close();
    return rows;


def get_segment_properties_in_ways(ways):

    if len(ways) == 0:
        return [];
    
    cur, conn = connect();

    strings = [str(x) for x in ways]
    ways_string = ','.join(strings);
    ways_string = '(' + ways_string + ')';

    sql = """
        SELECT "segment_properties"."id", "segment_properties"."segment", "segment_properties"."type", "segment_properties"."value"
        FROM "segment_properties" INNER JOIN "segments"
        ON "segments"."id" = "segment_properties"."segment"
        WHERE "segments"."way" IN {ways_string}
        """.format(ways_string=ways_string);
    cur.execute(sql);
    rows = cur.fetchall();
    conn.close();
    return rows;



def delete_all():
    cur, conn = connect();

    sql = """
        DELETE FROM "measurements";
        DELETE FROM "computed_values";
        DELETE FROM "aggregated_values";
        DELETE FROM "segments";
        DELETE FROM "segment_properties";
        DELETE FROM "computed_values_types";
        DELETE FROM "aggregation_methods"
        """
    cur.execute(sql);

    conn.commit();
    conn.close();
    return;

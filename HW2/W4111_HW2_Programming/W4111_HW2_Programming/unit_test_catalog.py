import CSVCatalog
import json
import pymysql

# Example test, you will have to update the connection info
# Implementation Provided

batting_columns = ["playerID","yearID","stint","teamID","lgID","G","AB","R","H"]
def create_table_test():

    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t = cat.create_table("test_table", "./testing.csv")
    t2 = cat.create_table("test_table2", "./testing2.csv")
    #t = cat.get_table("batting")
    print("Table = ", json.dumps(t.describe_table()))
    print("Table = ", json.dumps(t2.describe_table()))

#create_table_test()

def reset_db():

    conn = pymysql.connect(host = "localhost",
                          port = 3306,
                          user = "root",
                          password = "admin123",
                          db = "CSVCatalog")
    q = "delete from csvindexes"
    result = CSVCatalog.run_q(conn, q, args=None)

    q = "delete from csvtables"
    result = CSVCatalog.run_q(conn, q, args=None)

    q = "delete from csvcolumns"
    result = CSVCatalog.run_q(conn, q, args=None)

def test_1():
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")

    t = cat.create_table("batting", "./Batting.csv")
    for col in batting_columns:
        if col in ['playerID', "teamID","lgID"]:
            tt = 'text'
        else :
            tt = 'number'
        if col in ['playerID', "yearID", "stint"]:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(col, tt, n_null)

        t.add_column_definition(new_c)

    t.define_index("primary", ["playerID", "yearID", "stint"], "PRIMARY")

#reset_db()

#test_1()
def drop_table_test():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t2 = cat.drop_table("test_table2")

#drop_table_test()

def add_column_test():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t = cat.get_table("test_table")
    new_c = CSVCatalog.ColumnDefinition("player_age", 'number', False)
    new_c_2 = CSVCatalog.ColumnDefinition("playerID", 'number', True)
    t.add_column_definition(new_c)
    t.add_column_definition(new_c_2)

#add_column_test()

# Implementation Provided
# Fails because no name is given
def column_name_failure_test():
    cat = CSVCatalog.CSVCatalog()
    col = CSVCatalog.ColumnDefinition(None, "text", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_name_failure_test()

# Implementation Provided
# Fails because "canary" is not a permitted type
def column_type_failure_test():
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("bird", "canary", False)
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_type_failure_test()

# Implementation Provided
# Will fail because "happy" is not a boolean
def column_not_null_failure_test():
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    col = CSVCatalog.ColumnDefinition("name", "text", "happy")
    t = cat.get_table("test_table")
    t.add_column_definition(col)

#column_not_null_failure_test()


def add_index_test():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t = cat.get_table("test_table")
    t.define_index("player_index", ["playerID"], "INDEX")

#add_index_test()


def col_drop_test():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t = cat.get_table("test_table")
    t.drop_column_definition("player_age")

#col_drop_test()

def index_drop_test():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog(dbhost="localhost", dbport=3306, dbuser="root", dbpw="admin123", db="CSVCatalog")
    t = cat.get_table("test_table")
    t.drop_index("player_index")

#index_drop_test()

# Implementation provided
def describe_table_test():
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("test_table")
    desc = t.describe_table()
    print("DESCRIBE People = \n", json.dumps(desc, indent = 2))

#describe_table_test()


import CSVTable
import CSVCatalog
import json
import csv

#Must clear out all tables in CSV Catalog schema before using if there are any present
#Please change the path name to be whatever the path to the CSV files are
#First methods set up metadata!! Very important that all of these be run properly

# Only need to run these if you made the tables already in your CSV Catalog tests
# You will not need to include the output in your submission as executing this is not required
# Implementation is provided
def drop_tables_for_prep():
    cat = CSVCatalog.CSVCatalog()
    cat.drop_table("people")
    cat.drop_table("batting")
    cat.drop_table("appearances")

#drop_tables_for_prep()

# Implementation is provided
# You will need to update these with the correct path
def create_lahman_tables():
    cat = CSVCatalog.CSVCatalog()
    cat.create_table("people", "./People.csv")
    cat.create_table("batting","./Batting.csv")
    cat.create_table("appearances", "./Appearances.csv")

#create_lahman_tables()

# Note: You can default all column types to text
def update_people_columns():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("people")
    people_cols = ["playerID","birthYear","birthMonth","birthDay","birthCountry","birthState","birthCity","deathYear","deathMonth","deathDay",
                   "deathCountry","deathState","deathCity","nameFirst","nameLast","nameGiven","weight","height",
                   "bats","throws","debut","finalGame","retroID","bbrefID"]
    for col in people_cols:
        if col in ['playerID']:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(col, "text", n_null)

        t.add_column_definition(new_c)

#update_people_columns()

def update_appearances_columns():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("appearances")
    appearance_cols = ["yearID","teamID","lgID","playerID","G_all","GS","G_batting","G_defense","G_p","G_c","G_1b",
                   "G_2b","G_3b","G_ss","G_lf","G_cf","G_rf","G_of","G_dh","G_ph","G_pr"]
    for col in appearance_cols:
        if col in ['yearID', 'teamID', 'playerID']:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(col, "text", n_null)

        t.add_column_definition(new_c)

#update_appearances_columns()

def update_batting_columns():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    batting_columns = ["playerID", "yearID", "stint", "teamID", "lgID", "G", "AB", "R", "H", "2B","3B",
                       "HR","RBI","SB","CS","BB","SO","IBB","HBP","SH","SF","GIDP"]
    for col in batting_columns:
        if col in ['playerID', "yearID", "stint", "teamID"]:
            n_null = True
        else:
            n_null = False

        new_c = CSVCatalog.ColumnDefinition(col, "text", n_null)

        t.add_column_definition(new_c)

#update_batting_columns()

#Add primary key indexes for people, batting, and appearances in this test
def add_index_definitions():
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    t.define_index("primary", ["playerID", "yearID", "stint"], "PRIMARY")

    #cat = CSVCatalog.CSVCatalog()
    people = cat.get_table("people")
    people.define_index("primary", ["playerID"], "PRIMARY")

    appearances = cat.get_table("appearances")
    appearances.define_index("primary", ["playerID", "teamID", "yearID"], "PRIMARY")

#add_index_definitions()


def test_load_info():
    table = CSVTable.CSVTable("people")
    print(table.__description__.file_name)

#test_load_info()

def test_get_col_names():
    table = CSVTable.CSVTable("people")
    names = table.__get_column_names__()
    print(names)

#test_get_col_names()

def add_other_indexes():
    """
    We want to add indexes for common user stories
    People: nameLast, nameFirst
    Batting: teamID
    Appearances: None that are too important right now
    :return:
    """
    # ************************ TO DO ***************************
    cat = CSVCatalog.CSVCatalog()
    t = cat.get_table("batting")
    t.define_index("teamID_index", ["teamID"], "INDEX")

    # cat = CSVCatalog.CSVCatalog()
    people = cat.get_table("people")
    people.define_index("name_index", ["nameLast", "nameFirst"], "INDEX")

    # appearances = cat.get_table("appearances")
    # appearances.define_index("primary", ["playerID", "teamID", "yearID"], "PRIMARY")

#add_other_indexes()

def load_test():
    batting_table = CSVTable.CSVTable("batting")
    print(batting_table)

#load_test()


def dumb_join_test():
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")
    print(batting_table.__rows__[0])
    result = batting_table.dumb_join(appearances_table, ["playerID", "yearID"], {"playerID": "abercda01"},
                                     ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    print(result)


#dumb_join_test()


def get_access_path_test():
    batting_table = CSVTable.CSVTable("batting")
    template = ["stint", "playerID", "yearID"]
    index_result, count = batting_table.__get_access_path__(template)
    print(index_result)
    print(count)

#get_access_path_test()

def sub_where_template_test():
    # ************************ TO DO ***************************
    batting_table = CSVTable.CSVTable("batting")
    template = {"stint": "3", "playerID": "ras120", "yearID": "34567", "birthCity": "NYC"}
    print(batting_table.__get_sub_where_template__(template))

#sub_where_template_test()


def test_find_by_template_index():
    # ************************ TO DO ***************************

    batting_table = CSVTable.CSVTable("batting")

    template = {"stint" : "1", "playerID": "abercda01", "yearID": "1871"}
    index_result, count = batting_table.__get_access_path__(template)

    print(batting_table.__find_by_template_index__(template, index_result))
#test_find_by_template_index()

def smart_join_test():
    # ************************ TO DO ***************************
    batting_table = CSVTable.CSVTable("batting")
    appearances_table = CSVTable.CSVTable("appearances")
    print(batting_table.__rows__[0])
    result = batting_table.__smart_join__(appearances_table, ["playerID", "yearID"], {"playerID": "baxtemi01"},
                                        ["playerID", "yearID", "teamID", "AB", "H", "G_all", "G_batting"])
    # result = batting_table.__smart_join__(appearances_table, ["playerID", "yearID"], {"playerID": "abercda01", "teamID": "TRO",
    #                                                                                   "yearID" : "1871", "stint": "1"},
    #                                  ["playerID", "yearID", "teamID", "stint", "AB", "H", "G_all", "G_batting"])
    print(result)

smart_join_test()

def test_1():
    print("Starting from here")
    cat = CSVTable.CSVTable('batting', load=True)
    print(cat)

#test_1()
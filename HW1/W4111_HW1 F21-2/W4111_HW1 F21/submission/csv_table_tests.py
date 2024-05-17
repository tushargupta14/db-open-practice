"""

csv_table_tests.py

"""

from src.CSVDataTable import CSVDataTable

import os
import logging
import unittest

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
data_dir = os.path.abspath("../Data/Baseball")


def tests_people():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    people = CSVDataTable("People", connect_info, ["playerID"])
    try:

        print()
        print("find_by_primary_key(): Known Record")
        print(people.find_by_primary_key(["aardsda01"]))

        print()
        print("find_by_primary_key(): Unknown Record")
        print("Records found", people.find_by_primary_key((["cah2251"])))

        print()
        print("find_by_template(): Known Template")
        template = {"nameFirst": "David", "nameLast": "Aardsma", "nameGiven": "David Allan"}
        print(people.find_by_template(template))

        print()
        print("delete_by_key(): Known record")
        print("Before DELETE operation", len(people._rows))
        print("Num Rows deleted", people.delete_by_key(["aardsda01"]))
        print("After DELETE operation", len(people._rows))

        print()
        print("delete_by_key(): Unknown record")
        print("Before DELETE operation", len(people._rows))
        print("Num Rows deleted", people.delete_by_key(["cah2251"]))
        print("After DELETE operation", len(people._rows))

        print()
        print("delete_by_template(): Known Template")
        template = {"nameFirst": "Hank", "nameLast": "Aaron", "nameGiven": "Henry Louis"}
        print("Num rows deleted", people.delete_by_template(template))
        #assert people.delete_by_template(template) == 1

        print()
        print("update_by_key(): Known record")
        print("Before UPDATE operation")
        template = {"nameFirst": "Frank", "nameLast": "Abercrombie", "nameGiven": "Francis Patterson"}
        print(people.find_by_template(template))
        print("Num Rows updated", people.update_by_key(["abercda01"],
                                                       {"birthYear": "1980", "birthMonth": "2",
                                                        "birthDay": "14"}))
        print()
        print("update_by_key(): Unknown record")
        print("Num Rows updated", people.update_by_key(["cah2251"], {"birthYear": "1980", "birthMonth": "2",
                                                        "birthDay": "14"}))

        print()
        print("update_by_template(): Known record")
        print("Before UPDATE operation")
        template = {"birthYear": "1972", "birthMonth": "8", "birthDay": "17"}
        print(people.find_by_template(template))
        print("Num Rows updated", people.update_by_template(template,
                                                       {"birthYear": "1985", "birthMonth": "6",
                                                        "birthDay": "15"}))
        print("After UPDATE operation")
        print(people.find_by_primary_key(['abbotje01']))

        print()
        print("update_by_template(): Wrong field name in new_values()")
        print("Before UPDATE operation")
        template = {"birthYear": "1904", "birthMonth": "1", "birthDay": "1"}
        print(people.find_by_template(template))
        print("Num Rows updated", people.update_by_template(template,
                                                             {"birthYear": "1900", "birth_Month": "3", "birthDay": "30"}))

        print()
        print("update_by_key(): Modified value already exists in Primary Key")
        print("Before UPDATE operation")
        template = {"nameFirst": "Frank", "nameLast": "Abercrombie", "nameGiven": "Francis Patterson"}
        print(people.find_by_template(template))
        print("Num Rows updated", people.update_by_key(["abercda01"],
                                                       {"playerID": "abbotku01"}))
        print()
        print("insert(): Unique Primary key")
        print("Before INSERT operation")
        print("Num Rows", len(people._rows))
        people.insert({"playerID": "wusch3414", "yearID": "1980", "stint": "2"})
        print("After INSERT operation")
        print("Num Rows", len(people._rows))

        print()
        print("insert(): Duplicate Primary key")
        print("Before INSERT operation")
        print("Num Rows", len(people._rows))
        people.insert({"playerID": "abbotku01", "birthYear": "1985", "birthMonth": "6", "birthDay": "15"})
        print("After INSERT operation")
        print("Num Rows", len(people._rows))

        print()
        print("insert(): No Primary key specified")
        print("Before INSERT operation")
        print("Num Rows", len(people._rows))
        people.insert({"birthYear": "1985", "birthMonth": "6", "birthDay": "15"})
        print("After INSERT operation")
        print("Num Rows", len(people._rows))

    except Exception as e:
        print("An error occurred:", e)


def tests_batting():
    # Do the same tests for the batting table, so you can ensure your methods work for a table with a composite primary key
    # Replace this line with your tests

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }
    batting = CSVDataTable("Batting", connect_info, ["playerID", "yearID", "stint"])

    try:
        print()
        print("find_by_primary_key(): Known Record")
        print(batting.find_by_primary_key(["aardsda01", "2004", "1"]))

        print()
        print("find_by_primary_key(): Unknown Record")
        print("Records found", batting.find_by_primary_key((["cah2251", "2001", "2"])))

        print()
        print("find_by_template(): Known Template")
        template = {"teamID": "SFN", "lgID": "NL", "G": "11"}
        print(batting.find_by_template(template))

        print()
        print("delete_by_key(): Known record")
        print("Before DELETE operation", len(batting._rows))
        print("Num Rows deleted", batting.delete_by_key(["aardsda01", "2004", "1"]))
        print("After DELETE operation", len(batting._rows))

        print()
        print("delete_by_key(): Unknown record")
        print("Before DELETE operation", len(batting._rows))
        print("Num Rows deleted", batting.delete_by_key(["cah2251", "2001", "2"]))
        print("After DELETE operation", len(batting._rows))

        print()
        print("delete_by_template(): Known Template")
        template = {"teamID": "SFN", "lgID": "NL", "G": "11"}
        print("Num rows deleted", batting.delete_by_template(template))
        #assert people.delete_by_template(template) == 1

        print()
        print("update_by_key(): Known record")
        print("Before UPDATE operation")
        #template = {"teamID": "CHN", "lgID": "", "G": "11"}
        print(batting.find_by_primary_key(["aardsda01", "2006", "1"]))
        print("Num Rows updated", batting.update_by_key(["aardsda01", "2006", "1"],
                                                       {"teamID": "SFN", "lgID": "AL", "G": "20"}))
        print("After Update operation")
        print(batting.find_by_primary_key(["aardsda01", "2006", "1"]))

        print()
        print("update_by_key(): Unknown record")
        print("Num Rows updated", batting.update_by_key(["cah2251", "2001", "2"], {"teamID": "SFN", "lgID": "AL", "G": "20"}))

        print()
        print("update_by_template(): Known record")
        print("Before UPDATE operation")
        template = {"teamID": "CHA", "lgID": "AL", "G": "25"}
        print(batting.find_by_template(template))
        print("Num Rows updated", batting.update_by_template(template,
                                                       {"teamID": "NLA", "lgID": "FL", "G": "36"}))
        print("After UPDATE operation")
        print(batting.find_by_template({"teamID": "NLA", "lgID": "FL", "G": "36"}))

        print()
        print("update_by_template(): Wrong field name in new_values()")
        print("Before UPDATE operation")
        template = {"teamID": "ATL", "lgID": "NL", "G": "49"}
        print(batting.find_by_template(template))
        print("Num Rows updated", batting.update_by_template(template,
                                                             {"teamID": "ATL", "lg_ID": "FL", "G": "36"}))
        #print("After UPDATE operation")
        #print(batting.find_by_template({"teamID": "NLA", "lgID": "FL", "G": "36"}))

        print()
        print("update_by_key(): Modified value already exists in Primary Key field")
        print("Before UPDATE operation")
        #template = {"teamID": "CHA", "lgID": "AL", "G": "25"}
        print(batting.find_by_primary_key(["aardsda01", "2006", "1"]))
        print("Num Rows updated", batting.update_by_key(["aardsda01", "2006", "1"],
                                                       {"playerID": "aaronha01"}))
        print()
        print("insert(): Unique Primary key")
        print("Before INSERT operation")
        print("Num Rows", len(batting._rows))
        batting.insert({"playerID": "wqy2109a", "birthYear": "1985", "birthMonth": "6", "birthDay": "15"})
        print("After INSERT operation")
        print("Num Rows", len(batting._rows))

        print()
        print("insert(): Duplicate Primary key")
        print("Before INSERT operation")
        print("Num Rows", len(batting._rows))
        batting.insert({"playerID": "aaronha01", "yearID": "1959", "stint": "1"})
        print("After INSERT operation")
        print("Num Rows", len(batting._rows))

        print()
        print("insert(): No Primary key specified")
        print("Before INSERT operation")
        print("Num Rows", len(batting._rows))
        batting.insert({"G": "1985", "R": "6", "AB": "15"})
        print("After INSERT operation")
        print("Num Rows", len(batting._rows))

    except Exception as e:
        print("An error occurred:", e)

tests_people()
tests_batting()

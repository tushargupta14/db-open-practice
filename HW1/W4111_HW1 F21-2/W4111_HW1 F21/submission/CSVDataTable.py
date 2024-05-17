from src.BaseDataTable import BaseDataTable
import copy
import logging
import json
import os
import pandas as pd
import csv

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """
    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):

        """
        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._key_columns = key_columns

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        fn = self._data["connect_info"].get("directory") + "/" + self._data["connect_info"].get("file_name")
        with open(fn, "w") as csvfile:
            self.columns = self._rows[0].keys()
            csvw = csv.DictWriter(csvfile, self.columns)
            csvw.writeheader()
            for r in self._rows:
                csvw.writerow(r)

    def get_key_column(self):
        pkey = self._data.get("key_columns")
        return pkey

    @staticmethod
    def _project(row, field_list):

        result = {}

        if field_list is None:
            return row

        for f in field_list:
            result[f] = row[f]

        return result

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """
        Finds and returns the records that match the primary key
        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        # Create a template
        dictionary = dict(zip(self._key_columns, key_fields))

        # What method can you use?
        result = self.find_by_template(dictionary) if dictionary else []
        if len(result) > 0:
            # key should always return 1 result
            return result[0]
        else:
            return None

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """
        Finds the record that matches the template.
        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.

        """
        result = []

        for r in reversed(self._rows):
            if CSVDataTable.matches_template(r, template):
                new_r = CSVDataTable._project(r, field_list)
                result.append(new_r)

        return result

    def delete_by_key(self, key_fields):
        """
        Deletes the record that matches the key.
        :param key_fields: List of value for the key fields.
        :return: A count of the rows deleted.
        """

        # HINT: Create a dictionary of values/a template for key fields, then call a method you wrote
        #dictionary = dict(zip(self._key_columns, key_fields))

        template = self.find_by_primary_key(key_fields)
        if template is None:
            return 0
        else:
            count = self.delete_by_template(template)
        return count

    def delete_by_template(self, template):
        """
        Deletes the record that matches the template
        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        counter = 0

        # Iterate through rows, if matches, remove the row
        for row in reversed(self._rows):
            if CSVDataTable.matches_template(row, template):
                counter+=1
                self._rows.remove(row)
        return counter

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        # HINT: Create a dictionary of values/a template for key fields, then call a method you wrote
        template = self.find_by_primary_key(key_fields)
        if template is None:
            print("Searched key does not exist")
            return 0
        count = self.update_by_template(template, new_values)
        return count

    def update_by_template(self, template, new_values):
        """
        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        keys = set(new_values.keys())
        keys_table = set(self._rows[0].keys())

        if len(keys - keys_table) > 0:
            print("Error: Field to be modified does not exist")
            return 0

        counter = 0
        key_fields = []
        # Create key fields from the new values
        for k in self._key_columns:
            if k in new_values:
                if new_values[k] is '' or new_values[k] is None:
                    return counter
                key_fields.append(new_values[k])

        if self.find_by_primary_key(key_fields) is not None:
            print("Error: Value key is modified to already exists in table")
            return 0

        for i in range(len(self._rows)):
            if CSVDataTable.matches_template(self._rows[i], template):
                counter+=1
                for k,v in new_values.items():
                    self._rows[i][k] = v
        return counter

    def insert(self, new_record):
        """
        Inserts a new record
        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        key_fields = []
        for k in self._key_columns:
            v = new_record.get(k)
            if v is not None:
                key_fields.append(v)
        #print(key_fields)
        if not key_fields:
            print("Specify primary key to insert new record")
            return None
        record = {}
        self.columns = self._rows[0].keys()
        if self.find_by_primary_key(key_fields) is None:
            # Record can be inserted as no duplicate key exists
            for f in self.columns:
                record[f] = new_record.get(f, '')
            self._rows.append(record)
        else:
            print("Duplicate primary key found")
        return

    def get_rows(self):
        return self._rows

if __name__ == '__main__':

    csv_obj = CSVDataTable()
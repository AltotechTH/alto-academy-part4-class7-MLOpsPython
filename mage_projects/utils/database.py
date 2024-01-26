import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from crate import client


def _generate_sql_string_for_list(_list: list) -> str:
    """ Return string in a correct format for SQL query

    If list is empty, return empty string
    If list has one element, return "('DEVICE_1')"
    If list has more than one element, return "('DEVICE_1', 'DEVICE_2', ...)"

    Args:
        _list: List of items to be converted to SQL string

    Returns:
        String of list in a correct format for SQL query

    """
    if _list is None:
        return ""
    elif len(_list) == 0:
        return ""
    elif len(_list) == 1:
        return str(tuple(_list)).replace(',', '')  # Remove comma to avoid SQL syntax error ex. (3,) -> (3)
    else:
        return str(tuple(_list))


class AltoDatabase(ABC):
    """ Abstract class for Alto Database """

    def __init__(self, **kwargs):
        pass

    @abstractmethod
    def query_data(self, **kwargs):
        pass

    @abstractmethod
    def insert_data(self, **kwargs):
        pass


@dataclass
class AltoCrateDB(AltoDatabase):
    """ Class for Alto CrateDB """
    host: str = 'localhost'
    port: int = 4200
    username: str = None
    password: str = None

    def _add_where_clause(self, query_string: str, filters: dict):
        """
        Add the WHERE conditioning strings to the query string from the given filters dictionary

        Args:
            query_string (str): Query string to be modified
            filters (dict): Dictionary of filters to apply to the query with the format below

        Returns:
            query_string (str): Modified query string

        """
        prefix = ""

        if "WHERE" not in query_string:
            query_string += " WHERE "

        for col_name, f in filters.items():
            for oper, value in f.items():
                if value is None:
                    logging.debug(f"Invalid value for column [{col_name}] -- value = {value}")
                    continue

                # Preprocess value to be compatible with CrateDB query string
                if isinstance(value, str):
                    value_string = f"'{value}'"
                elif isinstance(value, list):
                    value_string = str(tuple(value)) if len(value) > 1 else str(tuple(value)).replace(",", "")
                else:
                    value_string = str(value)

                # Add filter to query string
                if oper in [">", "<", ">=", "<=", "=", "!=", "LIKE", "NOT LIKE"]:
                    query_string += f"{prefix}{col_name} {oper} {value_string}"
                    prefix = "\nAND "
                elif oper.upper() in ["IN", "NOT IN"] and isinstance(value, list):
                    query_string += f"{prefix}{col_name} {oper.upper()} {value_string}"
                    prefix = "\nAND "
                else:
                    print(f"Invalid filter specified for querying data from CrateDB -- {col_name}: {f}")

        return query_string

    def query_data(self, table_name: str, filters: dict):
        """
        Query data from CrateDB

        Args:
            table_name (str): Name of the table to query data from
            filters (dict): Dictionary of filters to apply to the query with the format below

            filters = {                             |   ex.     filters = {
                <column_name_1>: {                  |               timestamp: {
                    <operator_1>: <value_1>,        |                   ">": 100000,
                    <operator_2>: <value_2>,        |                   "<": 200000
                    ...                             |               },
                },                                  |               device_id: {
                <column_name_2>: ...                |                   "=": "device_1"
            }                                       |               }
                                                    |           }
        Supported operators: "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE"

        Returns:
            data (list): List of data from CrateDB. Each element is a dictionary with column name as keys.

             data = [{'timestamp': 1675245600000,
                    'location': 'chiller_plant/iot_devices',
                    'device_id': 'CSQ_plant',
                    'subdevice_idx': 0,
                    'type': 'calculated_power',
                    'aggregation_type': 'avg_1h',
                    'datapoint': 'power',
                    'value': '1289.8812590049934'}, ....]

        """
        query_string = f"SELECT * FROM {table_name}"

        # Step 1: Generate query string from the given filters dictionary
        if filters is not None:
            query_string = self._add_where_clause(query_string, filters)

        # Step 2: Query raw data from CrateDB
        logging.debug(f"Querying data from CrateDB: {query_string}")
        data: list = self.__execute_query_string(query_string)
        logging.debug(f"Finished querying data from CrateDB")

        if data is None:
            return []

        return data

    def insert_data(self, table_name: str, data: list):
        """ Insert data into CrateDB

        Args:
            table_name (str): Table name
            data (list[dict]): List of dictionaries. Each dictionary is a row of data

        """
        cratedb_url = str(self.host) + ':' + str(self.port)
        connection = client.connect(cratedb_url)
        cursor = connection.cursor()

        # Get column names from CrateDB
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        column_names = [row[0] for row in cursor.fetchall()]

        # Construct SQL insert command
        insert_string = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({','.join(['?'] * len(column_names))})"

        # Construct payload and execute many to insert data
        entry = []
        for row in data:
            entry += [tuple(row)]

        cursor.executemany(insert_string, entry)
        cursor.close()

    def delete_data(self, table_name: str, filters: dict):
        """
        Delete data from CrateDB

            Args:
                table_name (str): Name of the table to query data from
                filters (dict): Dictionary of filters to apply to the data deletion

                filters = {                             |   ex.     filters = {
                    <column_name_1>: {                  |               timestamp: {
                        <operator_1>: <value_1>,        |                   ">": 100000,
                        <operator_2>: <value_2>,        |                   "<": 200000
                        ...                             |               },
                    },                                  |               device_id: {
                    <column_name_2>: ...                |                   "=": "device_1"
                }                                       |               }
                                                        |           }
            Supported operators: "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "NOT LIKE"

        """
        # Step 1: Initialize cursor
        cratedb_url = str(self.host) + ':' + str(self.port)
        connection = client.connect(cratedb_url)
        cursor = connection.cursor()

        # Step 2: Generate SQL string to delete data
        sql_string = f"DELETE FROM {table_name}"
        sql_string = self._add_where_clause(sql_string, filters)

        # Step 3: Execute SQL string
        cursor.execute(sql_string)
        row_count = cursor.rowcount
        cursor.close()

        return row_count

    def count_data(self, table_name: str, filters: dict):
        """
            Args:
                table_name (str): Name of the table to query data from
                filters (dict): Dictionary of filters to apply to the data counting
        """
        # Step 1: Initialize cursor
        cratedb_url = str(self.host) + ':' + str(self.port)
        connection = client.connect(cratedb_url)
        cursor = connection.cursor()

        # Step 2: Generate SQL string to delete data
        sql_string = f"SELECT COUNT(*) FROM {table_name}"
        sql_string = self._add_where_clause(sql_string, filters)

        # Step 3: Execute SQL string
        cursor.execute(sql_string)
        count = cursor.fetchone()[0]

        return count

    def __execute_query_string(self, query_string: str):
        """
        Query data from CrateDB with specified query string.
        """
        cursor = None
        try:
            cratedb_url = str(self.host) + ':' + str(self.port)
            connection = client.connect(
                cratedb_url,
                username=self.username,
                password=self.password
            )
            cursor = connection.cursor()
            cursor.execute(query_string)
            datas = cursor.fetchall()
            connection.close()

            column_names = [desc[0] for desc in cursor.description]

            # Preprocess list-of-lists into list-of-dicts with correct keys and values
            res = list()
            for row in datas:
                res.append({k: v for k, v in zip(column_names, row)})

            return res

        except Exception as e:
            logging.debug(f"Data could not be queried: {e}")
        finally:
            if cursor:
                cursor.close()
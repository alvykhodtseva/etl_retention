# -*- coding: utf-8 -*-

from typing import Tuple, Any, List
import logging

import pandas as pd
from numpy import int64, float64

from bogoslovskiy.model.db.Implementation import InHouseDbWorker

from sqlalchemy.exc import SQLAlchemyError


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PostgresDataLoader:
	"""Class for uploading data into PotgresDb

	Attributes:
		db_worker (InHouseDbWorker): object to work with Postgres

	"""

	__slots__ = ("db_worker",)

	def __init__(self, db_worker: InHouseDbWorker):
		self.db_worker: InHouseDbWorker = db_worker

	@staticmethod
	def quotes_for_strings(elem: Any) -> str:
		"""Shitty function to provide quotes for strings

		Args:
			elem (Any): any element

		Returns:
			str: either string representation of the elem or string representation with double quotes from both ends

		"""

		if isinstance(elem, (int, int64, float64, float)) and not pd.isna(elem):
			return str(elem)

		elif (pd.isna(elem)) or (elem in ["NULL", 'NaT', 'None', 'nan', 'NaN']) or (elem is None):
			return 'NULL'

		else:
			return "\'" + str(elem) + "\'"

	def __get_table_columns(self, table_name: str) -> List[str]:
		"""Function gets columns names for a specified table via query

		Args:
			table_name (str): name of table in a database

		Returns:
			List[str]: columns in the specified table

		"""

		logger.debug("Method `__get_table_columns` was called")

		query: str = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name)

		result: list = [i[0] for i in self.db_worker.get_iterable(query)]

		return result

	def __get_table_constraints(self, table_name: str) -> List[str]:
		"""Function gets columns in primary key for a specified table via query

		Args:
			table_name (str): name of table in a database

		Returns:
			List[str]: columns in primary key for the specified table

		"""
		logger.debug("Method `__get_table_constraints` was called")

		query: str = """
			SELECT c.column_name, c.data_type
			FROM information_schema.table_constraints tc 
			JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
			JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
			  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
			WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{}'
		""".format(table_name)

		result: List[str] = [i[0] for i in self.db_worker.get_iterable(query)]

		return result

	def __upsert_to_postgres(self, df: pd.DataFrame, table: str) -> Tuple[bool, str]:
		"""Another shitty function here. It gets your pandas dataframe, creates an "INSERT ... ON CONFLICT" query string
		from it and executes this query on a specified table.

		Args:
			df (pd.DataFrame): data to be upserted
			table (str): table name

		Returns:
			Tuple[bool, str]: tuple with a success status (bool) and an error description (str)

		"""

		logger.debug("`upsert_to_postgres` method called")

		columns: List[str] = self.__get_table_columns(table)
		constraints: List[str] = self.__get_table_constraints(table)

		rows = [row[1:] for row in df[columns].itertuples()]

		v = ', '.join(
			[
				'(' + i + ')' for i in [
					','.join(map(self.quotes_for_strings, i)) for i in rows
				]
			]
		)

		columns_s = ', '.join(map(str, columns))

		query = '''
		INSERT INTO {0} ({1})
		VALUES {2}
		'''.format(table, columns_s, v)

		# c for constraints
		query += ' ON CONFLICT ({}) DO UPDATE SET '.format(', '.join(constraints))

		query += ', '.join([i + ' = excluded.{}'.format(i) for i in columns])

		query += ' ;'

		try:
			self.db_worker.get_iterable(query)
		except SQLAlchemyError as e:
			return False, e.args[0] + "\n\n{}\n".format(query)

		return True, ''

	def upload_data(self, table: str, data: pd.DataFrame) -> bool:
		"""Main method for uploading data. \n
		Under the hood it calls `__upsert_to_postgres` and then checks result.

		Args:
			table (str): table name to upsert data
			data (pd.DataFrame): data to upsert

		Returns:
			bool: success status for data uploading

		"""
		logger.debug("Method `upload_data` was called")

		result: Tuple[bool, str] = self.__upsert_to_postgres(data, table)

		if result[0]:
			logger.info("Upsertion went well")
			return True
		else:
			logger.warning("Upserting to {} went wrong. Error:\n{}".format(table, result[1]))
			return False

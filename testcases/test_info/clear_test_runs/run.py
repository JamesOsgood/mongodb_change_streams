from ChangeStreamBaseTest import ChangeStreamBaseTest
import random
import math
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)

	# Test entry point
	def execute(self):
		self.clear_test_runs()

	def validate(self):
		db = self.get_db_connection(dbname='tests')
		self.test_info_count = db.test_runs.count_documents({})
		self.assertThat('self.test_info_count == 0')


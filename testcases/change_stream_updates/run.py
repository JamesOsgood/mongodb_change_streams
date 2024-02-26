from ChangeStreamBaseTest import ChangeStreamBaseTest
import time
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_input'

	# Test entry point
	def execute(self):
		self.db = self.get_db_connection(dbname=self.db_name)
		cs_coll = self.db[self.cs_coll_name]

		DOCS_TO_UPDATE = 500

		sample = [
			{
				'$sample': {
					'size': 1
				}
			}
		]
		docs_updated = 0
		while docs_updated < DOCS_TO_UPDATE:
			# Sample a doc
			for doc in cs_coll.aggregate(sample):
				id = doc['_id']
				cs_coll.update_one({'_id': id}, {'$set' : { 'update' : docs_updated}})
				self.log.info(f'Updated {docs_updated}, {id}')
			self.wait(1.0)
			docs_updated += 1

	def validate(self):
		pass

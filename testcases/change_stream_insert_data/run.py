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
		collection = self.db[self.input_data_coll_name]
		cs_coll = self.db[self.cs_coll_name]
		cs_coll.drop()
		cs_coll.create_index('type')

		DOCS_TO_INSERT = 1000000
		BATCH_SIZE = 50000
		docs_inserted = 0
		current_batch = []

		# Test info
		test_info = self.create_test_info()
		test_marker = self.create_test_run_marker(test_info, True)
		cs_coll.insert_one(test_marker)

		for doc in collection.find({}).sort({'_id' : 1}):
			if docs_inserted > DOCS_TO_INSERT:
				break
			
			current_batch.append(doc)
			if len(current_batch) == BATCH_SIZE:
				last_index = len(current_batch) -1
				test_id = datetime.now().isoformat()
				current_batch[0]['type'] = 'batch_start'
				current_batch[0]['ts'] = time.perf_counter()

				current_batch[last_index]['test_id'] = test_id
				current_batch[last_index]['type'] = 'batch_end'

				cs_coll.insert_many(current_batch)
				docs_inserted += len(current_batch)
				self.log.info(f'Inserted {docs_inserted}')
				current_batch = []
				self.wait(1.0)

		self.inserted_count = docs_inserted

		test_marker = self.create_test_run_marker(test_info, False)
		cs_coll.insert_one(test_marker)


	def validate(self):
		db = self.get_db_connection()
		coll = db[self.cs_coll_name]
		self.cnt = coll.count_documents({'type' : { '$ne' : 'test_marker'}})
		self.assertThat('self.cnt == self.inserted_count')

from ChangeStreamBaseTest import ChangeStreamBaseTest
import time
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_input'
		self.thread = None
		self.addCleanupFunction(self.stop_cs_thread)

		# Used from CS Thread
		self.batch_start = None
		self.batch_received_count = 0
		self.ts_first_received = None

	# Cleanup
	def stop_cs_thread(self):
		if self.thread is not None:
			self.thread.stop()
			self.thread.join()
			self.thread = None

	# Test entry point
	def execute(self):
		db = self.get_db_connection(dbname=self.db_name)
		collection = db[self.input_data_coll_name]
		cs_coll = db[self.cs_coll_name]
		cs_coll.drop()

		self.thread = self.create_change_stream_thread(cs_coll, self.on_change_received)

		DOCS_TO_INSERT = 1000000
		BATCH_SIZE = 10000
		docs_inserted = 0
		current_batch = []
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
		db.client.close()
		self.stop_cs_thread()

	def validate(self):
		db = self.get_db_connection()
		coll = db[self.cs_coll_name]
		self.cnt = coll.count_documents({})
		self.assertThat('self.cnt == self.inserted_count')

	def on_change_received(self, log, change_event):
		doc = change_event['fullDocument']
		self.batch_received_count += 1
		if doc['type'] == 'batch_start':
			self.batch_start = doc
			self.ts_first_received = time.perf_counter()
		elif doc['type'] == 'batch_end':
			ts_last_received = time.perf_counter()
			ts_inserted = self.batch_start['ts']
			self.log.info(f"Batch of {self.batch_received_count} - Deltas - first {self.ts_first_received - ts_inserted:4f}, last first {ts_last_received - ts_inserted:4f}")
			self.batch_received_count = 0


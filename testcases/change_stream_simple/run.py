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

		DOCS_TO_INSERT = 100000
		BATCH_SIZE = 3000
		docs_inserted = 0
		current_batch = []
		for doc in collection.find({}).sort({'_id' : 1}):
			if docs_inserted > DOCS_TO_INSERT:
				break
			
			current_batch.append(doc)
			if len(current_batch) == BATCH_SIZE:
				ts = time.perf_counter()
				for cd in current_batch:
					cd['ts_inserted'] = ts
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
		ts_inserted = doc['ts_inserted']
		ts_received = time.perf_counter()
		self.log.info(f'Doc received, delta is {ts_received - ts_inserted:4f}')


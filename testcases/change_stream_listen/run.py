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
		self.db = None

	# Cleanup
	def stop_cs_thread(self):
		if self.db is not None:
			self.db.client.close()
			self.db = None

		if self.thread is not None:
			self.thread.stop()
			self.thread.join()
			self.thread = None

	# Test entry point
	def execute(self):
		self.db = self.get_db_connection(dbname=self.db_name)
		collection = self.db[self.input_data_coll_name]
		cs_coll = self.db[self.cs_coll_name]
		cs_coll.drop()

		self.thread = self.create_change_stream_thread(self.db, self.cs_coll_name, self.on_change_received)

		# Just wait
		done = False
		while not done:
			self.wait(60.0)
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
			log.info(f"Batch of {self.batch_received_count} - Deltas - first {self.ts_first_received - ts_inserted:4f}, last {ts_last_received - ts_inserted:4f}, batch time {ts_last_received - self.ts_first_received:4f}")
			self.batch_received_count = 0


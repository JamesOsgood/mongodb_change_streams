from ChangeStreamBaseTest import ChangeStreamBaseTest
import time
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128
import copy

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_input'
		self.thread = None
		self.addCleanupFunction(self.stop_cs_thread)

		# Used from CS Thread
		self.batch_start = None
		self.batch_received_count = {}
		for type in ['insert', 'update']:
			self.batch_received_count[type] = 0
		self.ts_first_received = None
		self.db = None
		self.test_marker = None
		self.test_results = []

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

		full_document = 'updateLookup'
		full_document_before_change = None
		USE_PREIMAGES = self.project.USE_PREIMAGES == 'Y'
		if USE_PREIMAGES:
			self.log.info('Using preimages')
			full_document = 'required'
			full_document_before_change = 'required'

		self.thread = self.create_change_stream_thread(self.db, 
												 self.cs_coll_name, 
												 self.on_change_received, 
												 full_document=full_document,
												 full_document_before_change=full_document_before_change)

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
		op_type = change_event['operationType']
		# self.log.info(change_event)
		if doc['type'] == 'test_marker':
			if doc['is_test_start']:
				self.test_marker = doc
				self.log.info(f"Starting test {doc['test_info']['test_id']}")			
			else:
				self.log.info(f"Finished test {doc['test_info']['test_id']}")	
				self.insert_test_run(self.test_marker['test_info'], self.test_results)
				self.test_marker = None
				self.test_results = []		
		elif doc['type'] == 'batch_start':
			self.batch_received_count[op_type] += 1
			self.batch_start = doc['batch']
			self.ts_first_received = time.perf_counter()
		elif doc['type'] == 'batch_end':
			self.batch_received_count[op_type] += 1
			ts_last_received = time.perf_counter()
			ts_inserted = self.batch_start['ts']
			
			# Metrics
			first_delta = self.ts_first_received - ts_inserted
			last_delta = ts_last_received - ts_inserted
			batch_time = ts_last_received - self.ts_first_received
			results = {}
			results['ts_first_received'] = self.ts_first_received
			results['batch_received_count'] = copy.deepcopy(self.batch_received_count)
			results['first_delta'] = first_delta
			results['last_delta'] = last_delta
			results['batch_time'] = batch_time
			
			batch_total = 0
			batch_desc = []
			for type in self.batch_received_count.keys():						
				batch_total += self.batch_received_count[type]
				batch_desc.append(f'{self.batch_received_count[type]} {type}')
				self.batch_received_count[type] = 0
			if batch_total != 1000:
				self.log.warn(doc)
			log.info(f"Batch of {batch_total} {', '.join(batch_desc)} - Deltas - first {first_delta:4f}, last {last_delta:4f}, batch time {batch_time:4f}")
			self.test_results.append(results)
		else:
			if op_type in self.batch_received_count.keys():
				self.batch_received_count[op_type] += 1

		# self.log.info(f'{change_event["operationType"]} - {self.batch_received_count}')


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
		self.db = None
		self.test_marker = None
		self.test_results = []

		self.updated_ids = {}
		self.test_batch_size = 0
		self.current_batch_count = 0

		self.ts_first_inserted = None
		self.ts_first_received = None

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

		full_document_before_change = None
		USE_FULL_DOCUMENT = self.project.FULL_DOCUMENT == 'Y'
		full_document = 'updateLookup' if USE_FULL_DOCUMENT else None
		
		self.thread = self.create_change_stream_thread(self.db, 
												 self.cs_coll_name, 
												 self.on_change_received, 
												 full_document=full_document)

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
		op_type = change_event['operationType']
		if op_type == 'insert':
			full_doc = change_event['fullDocument']
			self.handle_insert(log, op_type, full_doc)
		elif op_type == 'update':
			self.handle_update(log, op_type, change_event['updateDescription']['updatedFields'])
		else:
			log.warn(f'Unknown op_type = {op_type}')

		if self.current_batch_count == self.test_batch_size:
			self.handle_batch(log)
			self.reset_batch()

		# self.wait(0.5)

	def handle_insert(self, log, op_type, full_doc):
		# self.log.info(change_event)
		if full_doc['type'] == 'test_marker':
			if full_doc['is_test_start']:
				self.test_marker = full_doc
				self.test_batch_size = self.test_marker['test_info']['batch_size']
				self.log.info(f"Starting test {full_doc['test_info']['test_id']}, batch size {self.test_batch_size}")			
				self.reset_batch()
			else:
				self.log.info(f"Finished test {full_doc['test_info']['test_id']}")	
				self.insert_test_run(self.test_marker['test_info'], self.test_results)
				self.test_marker = None
				self.test_results = []		
		else:
			# log.info(f'INSERT: {full_doc}')
			self.update_op_type_count(log, op_type, full_doc['updated_ts'])

	def reset_batch(self):
		self.ts_first_received = None
		self.ts_first_inserted = None
		self.current_batch_count = 0

	def update_op_type_count(self, log, op_type, ts):
		if self.ts_first_received == None:
			self.ts_first_received = time.perf_counter()
			self.ts_first_inserted = ts
		
		if op_type in self.batch_received_count.keys():
			self.batch_received_count[op_type] += 1
			self.current_batch_count += 1

	def handle_update(self, log, op_type, updated_fields):
		# log.info(f'UPDATE: {full_doc}, {updated_fields}')
		self.update_op_type_count(log, op_type, updated_fields['updated_ts'])

	def handle_batch(self, log):
		ts_last_received = time.perf_counter()
		ts_inserted = self.ts_first_inserted
		
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
		if batch_total != int(self.project.INSERT_BATCH_SIZE):
			self.log.warn('Eek')
		log.info(f"Batch of {batch_total} {', '.join(batch_desc)} - Deltas - first {first_delta:4f}, last {last_delta:4f}, batch time {batch_time:4f}")
		self.test_results.append(results)



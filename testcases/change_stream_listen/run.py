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

		self.updated_ids = {}

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
		full_doc = change_event['fullDocument']
		op_type = change_event['operationType']
		if op_type == 'insert':
			self.handle_insert(log, op_type, full_doc)
		elif op_type == 'update':
			self.handle_update(log, op_type, full_doc, change_event['updateDescription']['updatedFields'])
		else:
			log.warn(f'Unknown op_type = {op_type}')

	def handle_insert(self, log, op_type, full_doc):
		# self.log.info(change_event)
		if full_doc['type'] == 'test_marker':
			if full_doc['is_test_start']:
				self.test_marker = full_doc
				self.log.info(f"Starting test {full_doc['test_info']['test_id']}")			
			else:
				self.log.info(f"Finished test {full_doc['test_info']['test_id']}")	
				self.insert_test_run(self.test_marker['test_info'], self.test_results)
				self.test_marker = None
				self.test_results = []		
		else:
			# log.info(f'INSERT: {full_doc}')
			self.update_op_type_count(log, op_type)
			if 'batch' in full_doc:
				self.handle_batch(log, op_type, full_doc)

	def update_op_type_count(self, log, op_type):
		if op_type in self.batch_received_count.keys():
			self.batch_received_count[op_type] += 1
			# log.info(f'{op_type} - {self.batch_received_count[op_type]}')

	def handle_update(self, log, op_type, full_doc, updated_fields):
		self.update_op_type_count(log, op_type)

		# log.info(f'UPDATE: {full_doc}, {updated_fields}')
		# id = full_doc['_id']
		# if id in self.updated_ids:
		# 	log.info(f'{id}: PREV_UPDATE - {self.updated_ids[id]}, THIS_UPDATE - {updated_fields}')
		# self.updated_ids[id] = updated_fields

		# See if the update includes the batch marker
		if 'batch' in updated_fields:
			self.handle_batch(log, op_type, full_doc)

	def handle_batch(self, log, op_type, full_doc):
		if full_doc['type'] == 'batch_start':
			self.batch_start = full_doc['batch']
			self.updated_ids = {}
			self.ts_first_received = time.perf_counter()
		elif full_doc['type'] == 'batch_end':
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
			if batch_total != int(self.project.INSERT_BATCH_SIZE):
				self.log.warn(full_doc)
			log.info(f"Batch of {batch_total} {', '.join(batch_desc)} - Deltas - first {first_delta:4f}, last {last_delta:4f}, batch time {batch_time:4f}")
			self.test_results.append(results)



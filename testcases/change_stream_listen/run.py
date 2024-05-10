from ChangeStreamBaseTest import ChangeStreamBaseTest
from datetime import datetime
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
		self.ts_prev_first_inserted = None

		self.ts_first_received = None
		self.ts_prev_first_received = None

		self.ts_last_received = None
		self.ts_prev_last_received = None

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
		input_collection = self.db[self.input_data_coll_name]
		cs_coll = self.db[self.cs_coll_name]
		if self.project.RUN_AGAINST_INPUT_COLLECTION == 'Y':
			cs_coll = input_collection
		else:
			cs_coll.drop()

		USE_FULL_DOCUMENT = self.project.FULL_DOCUMENT == 'Y'
		full_document = 'updateLookup' if USE_FULL_DOCUMENT else None
		
		self.thread = self.create_change_stream_thread(self.db, 
												 cs_coll.name,
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
			self.next_batch()

		# self.wait(0.5)

	def handle_insert(self, log, op_type, full_doc):
		# self.log.info(change_event)
		if full_doc['type'] == 'test_marker':
			if full_doc['is_test_start']:
				self.test_marker = full_doc
				self.test_batch_size = self.test_marker['test_info']['params']['batch_size']
				self.log.info(f"Starting test {full_doc['test_info']['test_id']}, batch size {self.test_batch_size}")			
				self.next_batch()
			else:
				self.log.info(f"Finished test {full_doc['test_info']['test_id']}")	
				self.insert_test_run(self.test_marker['test_info'], self.test_results)
				self.test_marker = None
				self.ts_prev_first_inserted = None
				self.ts_prev_last_inserted = None
				self.test_results = []		
		else:
			# log.info(f'INSERT: {full_doc}')
			self.update_op_type_count(log, op_type, full_doc['updated_ts']['ts'])

	def next_batch(self):
		self.ts_prev_first_inserted = self.ts_first_inserted
		self.ts_prev_first_received = self.ts_first_received

		self.ts_first_received = None
		self.ts_first_inserted = None
		self.ts_last_received = None
		self.current_batch_count = 0

	def update_op_type_count(self, log, op_type, ts_inserted):
		self.ts_last_inserted = ts_inserted
		self.ts_last_received = datetime.now()

		if self.ts_first_inserted == None:
			self.ts_first_inserted = self.ts_last_inserted

		if self.ts_first_received == None:
			self.ts_first_received = self.ts_last_received

		if op_type in self.batch_received_count.keys():
			self.batch_received_count[op_type] += 1
			self.current_batch_count += 1


	def handle_update(self, log, op_type, updated_fields):
		# log.info(f'UPDATE: {full_doc}, {updated_fields}')
		self.update_op_type_count(log, op_type, updated_fields['updated_ts']['ts'])

	def handle_batch(self, log):
		
		# Metrics
		results = {}
		results['batch_index'] = len(self.test_results)
		results['batch_contents'] = copy.deepcopy(self.batch_received_count)

		insert = {}
		insert['first'] = self.ts_first_inserted
		insert['last'] = self.ts_last_inserted
		if self.ts_prev_first_inserted is not None:
			insert['prev_first'] = self.ts_prev_first_inserted
			insert['batch_time'] = (self.ts_last_inserted - self.ts_first_inserted).total_seconds()
			insert['time_between_batches'] = (self.ts_first_inserted - self.ts_prev_first_inserted).total_seconds()

		results['insert'] = insert

		recv = {}
		recv['first'] = self.ts_first_received
		recv['last'] = self.ts_last_received
		if self.ts_prev_first_inserted is not None:
			recv['prev_first'] = self.ts_prev_first_received
			recv['batch_time'] = (self.ts_last_received - self.ts_first_received).total_seconds()
			recv['time_between_batches'] = (self.ts_first_received - self.ts_prev_first_received).total_seconds()

		results['receive'] = recv
		
		batch_total = 0
		batch_desc = []
		for type in self.batch_received_count.keys():						
			batch_total += self.batch_received_count[type]
			batch_desc.append(f'{self.batch_received_count[type]} {type}')
			self.batch_received_count[type] = 0
		if batch_total != int(self.project.INSERT_BATCH_SIZE):
			self.log.warn('Eek')
		log.info(f"Batch {results['batch_index']} received : {batch_total} {', '.join(batch_desc)}")

		self.test_results.append(results)



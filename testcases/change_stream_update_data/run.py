from ChangeStreamBaseTest import ChangeStreamBaseTest
import random
from datetime import datetime
from pymongo import InsertOne, UpdateOne
class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.batch_index = 0

	# Test entry point
	def execute(self):
		self.db = self.get_db_connection(dbname=self.db_name)
		collection = self.db[self.input_data_coll_name]
		collection.delete_many({'type' : 'test_marker'})
		doc_count = self.get_doc_count(collection)
		doc_id_range = range(doc_count)
		self.log.info(f'{doc_count} docs in input collection {self.input_data_coll_name}')

		DOCS_TO_INSERT = int(self.project.DOCS_TO_INSERT)
		BATCH_SIZE = int(self.project.INSERT_BATCH_SIZE)
		PRE_UPDATE_INSERT_COUNT = 0
		PERCENT_UPDATES = 100
		USE_FULL_DOCUMENT = self.project.FULL_DOCUMENT == 'Y'

		params = {}
		params['docs_to_insert'] = DOCS_TO_INSERT
		params['batch_size'] = BATCH_SIZE
		params['pre_update_insert_count'] = PRE_UPDATE_INSERT_COUNT
		params['percent_updates'] = PERCENT_UPDATES
		params['use_full_document'] = USE_FULL_DOCUMENT
		params['run_against_input_collection'] = 'Y'

		WAIT_TIME = 1.0
		docs_processed = 0
		ts_counter = 0

		# Test info
		test_info = self.create_test_info(params)
		test_marker = self.create_test_run_marker(test_info, True)
		collection.insert_one(test_marker)

		current_batch = []
		while docs_processed < DOCS_TO_INSERT:
			ids_to_update = random.sample(doc_id_range, BATCH_SIZE)
			for id_to_update in ids_to_update:
				ts_now, ts_counter = self.create_ts(datetime.now(), ts_counter)
				filter = { '_id' : id_to_update}
				updates = {}
				updates['type'] = 'doc'
				updates['updated_ts'] = ts_now
				current_batch.append(UpdateOne(filter, 
								{ '$set' : updates, 
									'$inc' : { 'version' : 1}, 
								}))
		
			if len(current_batch) == BATCH_SIZE:
				collection.bulk_write(current_batch)
				docs_processed += len(current_batch)
				self.log.info(f'Processed {docs_processed}')
				current_batch = []
			
			self.wait(WAIT_TIME)

		test_marker = self.create_test_run_marker(test_info, False)
		collection.insert_one(test_marker)

	def create_ts(self, ts_now, ts_counter):
		ts = { 'ts' : ts_now, 'ts_counter' : ts_counter }
		return (ts, ts_counter+1)

	def get_doc_count(self, coll):
		pipeline = [
			{
				'$count': 'cnt'
			}
		]

		res = list(coll.aggregate(pipeline))
		return res[0]['cnt']
	
	def validate(self):
		pass

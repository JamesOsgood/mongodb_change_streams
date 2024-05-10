from ChangeStreamBaseTest import ChangeStreamBaseTest
import random
from datetime import datetime
from pymongo import InsertOne, UpdateOne
class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_input'
		self.batch_index = 0

	def create_output_collection(self, db, coll_name, sharded):
		cs_coll = db.get_collection(coll_name)
		cs_coll.drop()
		cs_coll = self.db.create_collection(coll_name)
		cs_coll.create_index('type')

		if sharded:
			self.log.info(f'Sharding {cs_coll.name}')
			cs_coll.create_index('account')
			db.client.admin.command('enableSharding', db.name)
			full_name = f'{db.name}.{cs_coll.name}'
			db.client.admin.command('shardCollection', full_name, key={'account' : 1})

		return cs_coll

	# Test entry point
	def execute(self):
		self.db = self.get_db_connection(dbname=self.db_name)
		collection = self.db[self.input_data_coll_name]

		sharded = self.project.SHARD_OUTPUT_COLLECTION == 'Y'
		cs_coll = self.create_output_collection(self.db, self.cs_coll_name, sharded )

		DOCS_TO_INSERT = int(self.project.DOCS_TO_INSERT)
		BATCH_SIZE = int(self.project.INSERT_BATCH_SIZE)
		PRE_UPDATE_INSERT_COUNT = int(self.project.PRE_UPDATE_INSERT_COUNT)
		PERCENT_UPDATES = int(self.project.PERCENT_UPDATES)
		WAIT_TIME = 1.0
		doc_inserted = 0
		docs_processed = 0
		current_input_id = -1
		ts_counter = 0

		current_batch = []
		batch_count = {}
		for type in ['insert', 'update']:
			batch_count[type] = 0

		# Test info
		test_info = self.create_test_info(BATCH_SIZE)
		test_marker = self.create_test_run_marker(test_info, True)
		cs_coll.insert_one(test_marker)

		input_count = self.get_doc_count(collection)

		while docs_processed < DOCS_TO_INSERT and current_input_id < input_count:

			is_insert = True 
			if doc_inserted > PRE_UPDATE_INSERT_COUNT:
				if random.random() < PERCENT_UPDATES / 100:
					is_insert = False

			ts_now, ts_counter = self.create_ts(datetime.now(), ts_counter)
			if is_insert:
				# Get next input doc
				current_input_id += 1
				doc = collection.find_one({'_id' : current_input_id})
				doc['updated_ts'] = ts_now
				current_batch.append(InsertOne(doc))
				doc_inserted += 1
				# self.log.info(f'Inserting {current_input_id} - {doc}')
				batch_count['insert'] += 1
			else:
				id_to_update = random.randint(0, current_input_id)
				filter = { '_id' : id_to_update}
				updates = {}
				updates['type'] = 'doc'
				updates['updated_ts'] = ts_now
				current_batch.append(UpdateOne(filter, 
								{ '$set' : updates, 
									'$inc' : { 'version' : 1}, 
								}))
				batch_count['update'] += 1
			
			if len(current_batch) == BATCH_SIZE:
				cs_coll.bulk_write(current_batch)
				docs_processed += len(current_batch)
				self.log.info(f'Processed {docs_processed}')
				current_batch = []
				
				batch_desc = []
				for type in batch_count.keys():
					batch_desc.append(f'{batch_count[type]} {type}')
					batch_count[type] = 0
				self.log.info(f'Writing batch: {", ".join(batch_desc)}')

				self.wait(WAIT_TIME)

		test_marker = self.create_test_run_marker(test_info, False)
		cs_coll.insert_one(test_marker)

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

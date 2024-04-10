from ChangeStreamBaseTest import ChangeStreamBaseTest
import time
from datetime import datetime
import random
from pymongo import InsertOne, UpdateOne
class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_input'

	def create_output_collection(self, db, coll_name, sharded):
		USE_PREIMAGES = self.project.USE_PREIMAGES == 'Y'
		cs_coll = db.get_collection(coll_name)
		cs_coll.drop()
		if USE_PREIMAGES:
			self.log.info('Using preimages')
			cs_coll = self.db.create_collection(coll_name, changeStreamPreAndPostImages = { 'enabled' : True } )
		else:
			cs_coll = self.db.create_collection(coll_name)
			cs_coll.create_index('type')

		if sharded:
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

		DOCS_TO_INSERT = 100000
		BATCH_SIZE = int(self.project.INSERT_BATCH_SIZE)
		PRE_UPDATE_INSERT_COUNT = int(self.project.PRE_UPDATE_INSERT_COUNT)
		PERCENT_UPDATES = int(self.project.PERCENT_UPDATES)
		WAIT_TIME = 1.0
		doc_inserted = 0
		docs_processed = 0
		current_input_id = 0

		current_batch = []
		batch_count = {}
		for type in ['insert', 'update']:
			batch_count[type] = 0

		# Test info
		test_info = self.create_test_info()
		test_marker = self.create_test_run_marker(test_info, True)
		cs_coll.insert_one(test_marker)

		input_count = self.get_doc_count(collection)

		while docs_processed < DOCS_TO_INSERT and current_input_id < input_count:

			batch_details = {}
			if len(current_batch) == 0:
				# self.log.info('Batch Start')
				batch_details['type'] = 'batch_start'
				batch_details['ts'] = time.perf_counter()
			elif len(current_batch) == BATCH_SIZE - 1:
				# self.log.info('Batch End')
				batch_details['type'] = 'batch_end'
				batch_details['test_id'] = test_info['test_id']

			is_insert = True 
			if doc_inserted > PRE_UPDATE_INSERT_COUNT:
				if random.random() < PERCENT_UPDATES / 100:
					is_insert = False

			if is_insert:
				# Get next input doc
				doc = collection.find_one({'_id' : current_input_id})
				current_input_id += 1
				if len(batch_details) > 0:
					doc['type'] = batch_details['type']
					doc['batch'] = batch_details
				current_batch.append(InsertOne(doc))
				doc_inserted += 1
				# self.log.info(f'Inserting {current_input_id} - {doc}')
				batch_count['insert'] += 1
			else:
				id_to_update = random.randint(0, current_input_id)
				filter = { '_id' : id_to_update}
				updates = { 'updated' : True}
				if len(batch_details) > 0:
					updates['type'] = batch_details['type']
					updates['batch'] = batch_details
					# self.log.info(f'Updating {id_to_update} = {updates}')
					current_batch.append(UpdateOne(filter, { '$set' : updates, '$inc' : { 'version' : 1}}))
				else:
					updates['type'] = 'doc'
					# self.log.info(f'Updating {id_to_update} = {updates}')
					current_batch.append(UpdateOne(filter, { '$set' : updates, '$inc' : { 'version' : 1}, '$unset' : { 'batch' : ''  }}))
				batch_count['update'] += 1
			
			if len(current_batch) == BATCH_SIZE:
				# if doc_inserted > PRE_UPDATE_INSERT_COUNT:
				# 	for doc in current_batch:
				# 		self.log.info(doc)
				# 		cs_coll.bulk_write([doc])
				# 		self.wait(1.0)
				# else:
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

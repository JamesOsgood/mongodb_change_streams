from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pysys.basetest import BaseTest
from datetime import datetime
class ChangeStreamBaseTest(BaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		self.connectionString = self.project.CONNECTION_STRING.replace("~", "=")
		self.process_index = -1
		self.file_index = -1
		self.db_name = 'cs_test'
		self.input_data_coll_name = 'input_data'

	# open db connection
	def get_db_connection(self, dbname = None):
		client = MongoClient(self.connectionString)
		if dbname:
			db_connection = client.get_database(dbname)
		else:
			db_connection = client.get_database()

		return db_connection

	def create_test_info(self):
		db = self.get_db_connection()
		# Mongo Info
		status = db.command('serverStatus')
		mongo = {}
		mongo['version'] = status['version']

		sharded = False
		if status['process'] == 'mongos':
			sharded = True
		mongo['sharded'] = sharded

		# Server info
		host_info = db.command('hostInfo')
		system = host_info['system']
		host = {}
		host['name'] = system['hostname']
		host['cores'] = system['numCores']
		host['memSizeMB'] = system['memSizeMB']
		host['memSizeGB'] = host['memSizeMB'] / 1000
		
		test_info = {}
		test_info['test_id'] = datetime.now().isoformat()
		test_info['mongo'] = mongo
		test_info['host'] = host
		return test_info

	def create_test_run_marker(self, test_info, is_start):
		doc = {}
		doc['type'] = 'test_marker'
		doc['test_info'] = test_info
		doc['is_test_start'] = is_start
		return doc
	
	def clear_test_runs(self):
		db = self.get_db_connection(dbname='tests')
		db.test_runs.drop()

	def insert_test_run(self, test_info, test_results):
		doc = {}
		doc['test_info'] = test_info
		doc['results'] = test_results
		db = self.get_db_connection(dbname=self.db_name)
		db.test_runs.insert_one(doc)

	def create_change_stream_thread(self, db, coll_name, on_change_received, full_document = None, fullDocumentBeforeChange=None):
		args = {}
		args['db'] = db
		args['coll_name'] = coll_name
		args['full_document'] = full_document
		args['fullDocumentBeforeChange'] = full_document
		args['on_change_received'] = on_change_received
		return self.startBackgroundThread('Change Stream consumer', self.change_stream_listener, kwargsForTarget=args)

	def change_stream_listener(self, stopping, log, db, coll_name, full_document, fullDocumentBeforeChange, on_change_received):
		resume_token = None
		running = not stopping.is_set()
		while running:
			try:
				pipeline = [{'$match': {'operationType': { '$in' : ['insert', 'update', 'invalidate']}}}]
				coll = db[coll_name]
				log.info(f'Creating change stream for {coll_name}')
				with coll.watch(pipeline, resume_after=resume_token, full_document=full_document, full_document_before_change=fullDocumentBeforeChange) as stream:
					for change in stream:
						opType = change['operationType']
						if opType == 'invalidate':
							log.info('Change stream is invalid, restarting')
							resume_token = None
							break
						elif opType == 'insert':
							on_change_received(log, change)
						elif opType == 'update':
							on_change_received(log, change)
						else:
							log.error(f'Unknown opType - {opType}')
						resume_token = stream.resume_token
						if stopping.is_set():
							running = False
							break
			except PyMongoError as ex:
				if running:
					# The ChangeStream encountered an unrecoverable error or the
					# resume attempt failed to recreate the cursor.
					if resume_token is None:
						# There is no usable resume token because there was a
						# failure during ChangeStream initialization.
						log.error(ex)
						log.error('No resume token so stopping')
						running = False
					else:
						running = not stopping.is_set()
						if running:
							log.error(ex)


from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pysys.basetest import BaseTest

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

	def create_change_stream_thread(self, coll, on_change_received):
		args = {}
		args['coll'] = coll
		args['on_change_received'] = on_change_received
		return self.startBackgroundThread('Change Stream consumer', self.change_stream_listener, kwargsForTarget=args)

	def change_stream_listener(self, stopping, log, coll, on_change_received):
		resume_token = None
		running = not stopping.is_set()
		while running:
			try:
				pipeline = [{'$match': {'operationType': 'insert'}}]
				with coll.watch(pipeline, resume_after=resume_token) as stream:
					for insert_change in stream:
						on_change_received(log, insert_change)
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


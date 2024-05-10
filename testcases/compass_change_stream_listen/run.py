from ChangeStreamBaseTest import ChangeStreamBaseTest
from datetime import datetime
from bson.decimal128 import Decimal128
import copy

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'compass_input'
		self.thread = None
		self.addCleanupFunction(self.stop_cs_thread)

		# Used from CS Thread
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
		cs_coll = self.db[self.cs_coll_name]

		self.thread = self.create_change_stream_thread(self.db, 
												 self.cs_coll_name, 
												 self.on_change_received, 
												 full_document=None)

		# Just wait
		done = False
		while not done:
			self.wait(60.0)
		self.stop_cs_thread()

	def validate(self):
		pass

	def on_change_received(self, log, change_event):
		op_type = change_event['operationType']
		if op_type == 'insert':
			full_doc = change_event['fullDocument']
			self.log.info(f'INSERT: fullDocument - {full_doc}')
		elif op_type == 'update':
			self.log.info(f"UPDATE: updatedFields - {change_event['updateDescription']}")
		else:
			log.warn(f'Unknown op_type = {op_type}')


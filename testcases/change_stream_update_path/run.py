from ChangeStreamBaseTest import ChangeStreamBaseTest
import time
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)
		self.cs_coll_name = 'cs_output_events'
		self.thread = None
		self.addCleanupFunction(self.stop_cs_thread)

		self.input_events = {}
		self.output_events = {}

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
		cs_coll.drop()

		self.thread = self.create_change_stream_thread(self.db, self.cs_coll_name, self.on_change_received)
		self.wait(1.0)

		# Simple doc
		doc = { 'field' : 1}
		res = cs_coll.insert_one(doc)
		cs_coll.update_one({'_id' : res.inserted_id}, { '$set' : { 'field' : 2}})

		# Nested doc with dotted path
		doc = { 'field' : { 'level' : 1}}
		res = cs_coll.insert_one(doc)
		cs_coll.update_one({'_id' : res.inserted_id}, { '$set' : { 'field.level' : 2}})

		# Nested doc with full sub doc
		doc = { 'field' : { 'level' : 1}}
		res = cs_coll.insert_one(doc)
		cs_coll.update_one({'_id' : res.inserted_id}, { '$set' : { 'field' : { 'level' : 2}}})

		# Nested doc with deeply dotted path
		doc = { 'field' : { 'sub' : { 'level' : 1}}}
		res = cs_coll.insert_one(doc)
		cs_coll.update_one({'_id' : res.inserted_id}, { '$set' : { 'field.sub.level' : 2}})

		# Just wait
		self.wait(2.0)
		self.stop_cs_thread()

	def validate(self):
		db = self.get_db_connection()
		coll = db[self.cs_coll_name]
		pass

	def on_change_received(self, log, change_event):
		opType = change_event['operationType']
		if opType == 'update':
			key = change_event['documentKey']
			id = key['_id']
			self.output_events[id] = change_event
			log.info(change_event)


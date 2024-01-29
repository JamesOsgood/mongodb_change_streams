from ChangeStreamBaseTest import ChangeStreamBaseTest
import random
import math
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)

	# Test entry point
	def execute(self):
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
		
		# db.status.drop()
		# db.status.insert_one(host)
		test_info = {}
		test_info['mongo'] = mongo
		test_info['host'] = host
		
		db.test_runs.drop()
		db.test_runs.insert_one(test_info)

	def validate(self):
		pass


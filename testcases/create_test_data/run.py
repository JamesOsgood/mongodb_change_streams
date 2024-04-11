from ChangeStreamBaseTest import ChangeStreamBaseTest
from datetime import datetime, timedelta
from bson.decimal128 import Decimal128
from faker import Faker
import random

class PySysTest(ChangeStreamBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		ChangeStreamBaseTest.__init__(self, descriptor, outsubdir, runner)

		self.coll_name = 'input_data'
		self.faker = Faker()

	# Test entry point
	def execute(self):
		db = self.get_db_connection(dbname=self.db_name)

		DOCUMENTS_TO_CREATE = 1000000
		accounts = 1000
		self.generate_documents(db, DOCUMENTS_TO_CREATE, accounts)

	def generate_documents(self, db, docs_to_generate, accounts):
		collection = db[self.input_data_coll_name]
		collection.drop()

		small_docs = self.project.SMALL_TEST_DOCS == 'Y'

		BUCKET_COUNT = 1000
		inserted_count = 0
		doc_index = 0
		current_bucket = []
		while inserted_count < docs_to_generate:
			doc = self.create_doc(doc_index, accounts, small_docs)
			current_bucket, inserted_count = self.store_doc(collection, doc, current_bucket, inserted_count, BUCKET_COUNT)
			doc_index += 1

		if len(current_bucket) > 0:
			collection.insert_many(current_bucket)
			self.log.info(f'Inserted {inserted_count + len(current_bucket)} docs')
			inserted_count += len(current_bucket)

		self.inserted_count = inserted_count

	def store_doc(self, coll, doc, current_bucket, inserted_count, bucket_count):
		current_bucket.append(doc)
		if len(current_bucket) >= bucket_count:
			coll.insert_many(current_bucket)
			self.log.info(f'Inserted {inserted_count + len(current_bucket)} docs')
			return ([], inserted_count + len(current_bucket))
		else:
			return (current_bucket, inserted_count)


	def create_doc(self, inserted_count, accounts, small_docs):
		doc = {}
		doc['_id'] = inserted_count
		doc['type'] = 'doc'
		doc['account'] = random.randint(0, accounts)

		if not small_docs:
			FLOAT_FIELD_COUNT = 130
			for index in range(FLOAT_FIELD_COUNT):
				doc[f'int_field_{index}'] = float(index)
			STRING_FIELD_COUNT = 44
			for index in range(STRING_FIELD_COUNT):
				doc[f'str_field_{index}'] = self.faker.word()
		return doc

	def validate(self):
		db = self.get_db_connection()
		coll = db[self.coll_name]
		self.cnt = coll.count_documents({})
		self.assertThat('self.cnt == self.inserted_count')


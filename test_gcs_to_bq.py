import unittest
import main
import csv
import gcsfs
import yaml

class TestDataLoad(unittest.TestCase):

	def setUp(self):
		#We have a config file for the cloud function which we can reuse
		env = {}
		with open("env.yaml", 'r') as stream:
			env = yaml.safe_load(stream)
		
		self.project = env['PROJECT'] 
		#We have to hard code the inputBucket as this value will come dynamically in cloud function event, (Also in cloud function config)
		self.inputBucket = 'my-project-1548066830003-data'
		self.outputBucket = env['OUTPUT_BUCKET']
		self.bqTableId = '{}.{}.{}'.format(env['PROJECT'] , env['BQ_DATASET_NAME'], env['BQ_TABLE_NAME'])


	def test_bad_csv(self):
		#1st test case - uploading an empty file / csv with bad format.
		inputFileName = 'empty.csv'
		self.assertIsNone(main.create_csv_with_ingest_time(self.project, self.inputBucket, inputFileName, self.outputBucket), msg="Error Not Captured")

	def test_match_record_count(self):
		#2nd test case - uploading the origin file and checking the row count in the csv and BQ table
		inputFileName = 'sample.csv'
		outputFilePath = main.create_csv_with_ingest_time(self.project, self.inputBucket, inputFileName, self.outputBucket)
		#Output of the create_csv function should not be empty
		self.assertIsNotNone(outputFilePath, msg="Error occured during csv read/write")
		if outputFilePath is not None:
			numberOfRows = main.load_csv_to_bigQuery(outputFilePath, self.bqTableId)
			#Big Query Load job should be successful and the number of records inserted should not be 0
			self.assertNotEqual(numberOfRows, 0, msg="BQ table not populated")	

			inputFileRowCount = 0
			#Get the row count for our csv file
			fs = gcsfs.GCSFileSystem(project=self.project)
			with fs.open('{}/{}'.format(self.inputBucket, inputFileName),'r') as csvinput:
				reader = csv.reader(csvinput, delimiter=';')
				#We need to ignore the header row, hence -1
				inputFileRowCount = len(list(reader)) - 1
			#Number of rows in the csv should be equal to the number of rows inserted in BigQuery
			self.assertEqual(inputFileRowCount, numberOfRows, msg="Row missmatch")

	def test_unequal_columns(self):
		#3rd test case - uploading a csv with an extra column - the csv with ingest time should be successful
		#But the BQ load job should fail due to inconsistent columns
		inputFileName = 'sample_extra_column.csv'
		outputFilePath = main.create_csv_with_ingest_time(self.project, self.inputBucket, inputFileName, self.outputBucket)		
		#Confirm that 1st stage was successful
		self.assertIsNotNone(outputFilePath, msg="Error occured during csv read/write")
		if outputFilePath is not None:
			#The result of the load to BQ function should be 0
			self.assertEqual(main.load_csv_to_bigQuery(outputFilePath, self.bqTableId), 0, msg="BQ table populated")


if __name__ == '__main__':
	unittest.main()
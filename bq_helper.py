from google.cloud import bigquery

class BqClientHelper():
	"""
		BQ Helper class to load data into Bigquery
	"""

	def __init__(self):
		# Create bigquery client
		self._client = bigquery.Client()
		
		
	def load_file(self, sql_path):
		""" Load plain text file from path """
		with open(sql_path, "r") as out_f:
			return out_f.read()

			
	def create_table_from_query(self, table_id, sql_path, write_disposition='WRITE_TRUNCATE', partition=None):
		""" Create BQ table using query """

		if partition:
			table_id = table_id + "${}".format(partition)
		
		sql = self.load_file(sql_path)
		
		print("Execute query : {}".format(sql))
		
		job_config = bigquery.QueryJobConfig(
			destination=table_id, 
			write_disposition=write_disposition, 
		)
		if partition:
			job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)

		# Launch the query
		query_job = self._client.query(sql, job_config=job_config)
		
		# Wait for the job to complete.
		query_job.result()  

		print("Query results loaded to the table {}".format(table_id))	
		
		
	def create_table_from_csv(self, table_id, path, write_disposition='WRITE_TRUNCATE', partition=None):
		""" Create table from CSV file, with schema autodetection """
		
		if partition:
			table_id = table_id + "${}".format(partition)
		
		# Load CSV with schema autodetection to be lazy (would be better with a fixed schema, especially in production)
		job_config = bigquery.LoadJobConfig(
			source_format=bigquery.SourceFormat.CSV,
			write_disposition=write_disposition,
			autodetect=True
		)
		if partition:
			job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
		
		# Load BQ table from file
		with open(path, "rb") as source_file:
			job = self._client.load_table_from_file(source_file, table_id, job_config=job_config)
			
		# Waits for the job to complete.
		job.result()  

		# Retrieve tables stats for logging
		table = self._client.get_table(table_id)
		print(
			"Loaded {} rows and {} columns to {}".format(
				table.num_rows, len(table.schema), table_id
			)
		)
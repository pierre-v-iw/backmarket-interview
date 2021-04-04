import urllib.request

from bq_helper import BqClientHelper

project_id = 'fabled-archive-306817'

conf = {
	'raw': '{}.backmarket.catalog_raw'.format(project_id)
	'ftA': '{}.backmarket.catalog_fta'.format(project_id)
	'ftB': '{}.backmarket.catalog_ftb'.format(project_id)
}

def download_file(url, file_name):
	""" Retrieve file from path, to get s3-hosted public file (no need to use the s3 API) """
	logging.info("Download file : " + url)
	urllib.request.urlretrieve(url, file_name)


def load_catalog(input_data):
	local_file = "./product_catalog.csv"
	
	bq = BqClientHelper()
	
	# First, retrieve the file locally
	download_file(input_data, local_file)
	
	# Insert it as raw data table in Bigquery
	bq.create_table_from_csv(conf['raw'], local_file)
	
	# Create table for FT A from raw data
	bq.create_table_from_query(conf['ftA'], "./sql/product_catalog_with_image.sql")
	
	# Create table for FT B from raw data
	bq.create_table_from_query(conf['ftB'], "./sql/product_catalog_samsung_without_image.sql")

	
if __name__ == "__main__":
	""" Load catalog in Bigquery. We suppose credentials are setup in the environment"""
	input_data = "https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv"
	load_catalog(input_data)

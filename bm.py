import urllib.request

from bq_helper import BqClientHelper

def download_file(url, file_name):
	""" Retrieve file from path, to get s3-hosted public file (no need to use the s3 API) """
	logging.info("Download file : " + url)
	urllib.request.urlretrieve(url, file_name)

	
if __name__ == "__main__":
	bq = BqClientHelper()

	input_data = "https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv"
	
	local_file = "./product_catalog.csv"
	
	# First, retrieve the file locally
	download_file(input_data, local_file)
	
	# Insert it as raw data table in Bigquery
	bq.create_table_from_csv("fabled-archive-306817.backmarket.catalog_raw", local_file)
	
	# Create table for FT A from raw data
	bq.create_table_from_query("fabled-archive-306817.backmarket.catalog_fta", "./sql/product_catalog_with_image.sql")
	
	# Create table for FT B from raw data
	bq.create_table_from_query("fabled-archive-306817.backmarket.catalog_ftb", "./sql/product_catalog_samsung_without_image.sql")
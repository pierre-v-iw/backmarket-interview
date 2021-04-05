import urllib.request
import argparse

from bq_helper import BqClientHelper

def download_file(url, file_name):
	""" Retrieve file from path, to get s3-hosted public file (no need to use the s3 API) """
	print("Download file : " + url)
	urllib.request.urlretrieve(url, file_name)


def load_catalog(input_data, conf):
	local_file = "./product_catalog.csv"
	
	bq = BqClientHelper()
	
	# First, retrieve the file locally
	download_file(input_data, local_file)
	
	# Insert it as raw data table in Bigquery
	bq.create_table_from_csv(conf['raw'], local_file)
	
	# Create table for FT A from raw data
	bq.create_table_from_query(conf['ftA'], conf['raw'], "./sql/product_catalog_with_image.sql")
	
	# Create table for FT B from raw data
	bq.create_table_from_query(conf['ftB'], conf['raw'], "./sql/product_catalog_samsung_without_image.sql")

	
if __name__ == "__main__":
	""" Load catalog in Bigquery. We suppose credentials are setup in the environment"""
	input_data = "https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv"
	
	parser = argparse.ArgumentParser(description='BigQuery import')
	parser.add_argument('--project', dest='project', type=str, nargs='?', default ='fabled-archive-306817', help='BigQuery project to load data')
	parser.add_argument('--dataset', dest='dataset', type=str, nargs='?', default ='backmarket', help='BigQuery dataset to load data')
	
	args = parser.parse_args()
	
	# Build configuration from arguments
	conf = {
		'raw': '{}.{}.catalog_raw'.format(args.project, args.dataset),
		'ftA': '{}.{}.catalog_fta'.format(args.project, args.dataset),
		'ftB': '{}.{}.catalog_ftb'.format(args.project, args.dataset)
	}
	
	# Load catalog files in BQ
	load_catalog(input_data, conf)

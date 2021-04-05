import argparse
import urllib.request

from bq_helper import BqClientHelper

def load_catalog(path, conf, date):
	""" Load a raw catalog file into BigQuery partitioned table and apply downstream processing """
	file = path.format(date)
	
	bq = BqClientHelper()
	
	# Insert data as raw data partition table in Bigquery
	bq.create_table_from_csv(conf['raw'], file, partition=date)
	
	# Create partition in table for FT A from raw data
	bq.create_table_from_query(conf['ftA'], "./sql/product_catalog_with_image.sql", partition=date)
	
	# Create partition in table for FT B from raw data
	bq.create_table_from_query(conf['ftB'], "./sql/product_catalog_samsung_without_image.sql", partition=date)	


if __name__ == "__main__":
	""" Load catalog in Bigquery. We suppose credentials are setup in the environment"""
	dates = ["20210403", "20210404"]

	parser = argparse.ArgumentParser(description='BigQuery import')
	parser.add_argument('--project', dest='project', type=str, nargs='?', default ='fabled-archive-306817', help='BigQuery project to load data')
	parser.add_argument('--dataset', dest='dataset', type=str, nargs='?', default ='backmarket', help='BigQuery dataset to load data')
	
	args = parser.parse_args()
	
	# Build configuration from arguments
	conf = {
		'raw': '{}.{}.catalog_inc_raw'.format(args.project, args.dataset),
		'ftA': '{}.{}.catalog_inc_fta'.format(args.project, args.dataset),
		'ftB': '{}.{}.catalog_inc_ftb'.format(args.project, args.dataset)
	}	
	
	local_file = "./product_catalog_{}.csv"
	for d in dates:
		print("Load catalog data for date={}".format(d))
		# This should be seen as an ETL job executed once a day (and which can be re-executed as many as times as wanted, see loop as a airflow backfill)
		load_catalog(local_file, conf, date=d)

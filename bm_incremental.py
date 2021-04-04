import urllib.request

from bq_helper import BqClientHelper

project_id = 'fabled-archive-306817'

conf = {
	'raw': '{}.backmarket.catalog_inc_raw'.format(project_id),
	'ftA': '{}.backmarket.catalog_inc_fta'.format(project_id),
	'ftB': '{}.backmarket.catalog_inc_ftb'.format(project_id)
}

def load_catalog(path, date):
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

	local_file = "./product_catalog_{}.csv"
	
	for d in dates:
		print("Load catalog data for date={}".format(d))
		# This should be seen as an ETL job executed once a day (and which can be re-executed as many as times as wanted, see loop as a airflow backfill)
		load_catalog(local_file, date=d)

import urllib.request

from datetime import datetime

from bq_helper import BqClientHelper

def load_catalog(path, date):
	file = path.format(date)
	
	# Insert it as raw data partition table in Bigquery
	bq.create_table_from_csv("fabled-archive-306817.backmarket.catalog_inc_raw", file, partition=date)
	
	# Create partition in table for FT A from raw data
	bq.create_table_from_query("fabled-archive-306817.backmarket.catalog_inc_fta", "./product_catalog_with_image.sql", partition=date)
	
	# Create partition in table for FT B from raw data
	bq.create_table_from_query("fabled-archive-306817.backmarket.catalog_inc_ftb", "./product_catalog_samsung_without_image.sql", partition=date)	

	
if __name__ == "__main__":
	bq = BqClientHelper()
	
	dates = ["20210403", "20210404"]

	local_file = "./product_catalog_{}.csv"
	
	for d in dates:
		# This should be seen as an ETL job executed once a day (and which can be re-executed as many as times as wanted, see loop as a airflow backfill)
		load_catalog(local_file, date=d)

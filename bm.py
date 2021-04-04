import urllib.request

from datetime import datetime

def download_file(url, file_name):
	""" Retrieve file from path, to get s3-hosted public file (no need to use the s3 API) """
	logging.info("Download file : " + url)
	urllib.request.urlretrieve(url, file_name)

if __name__ == "__main__":
	input_data = "https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv"
	
	local_file = "./product_catalog.csv"
	
	# First, retrieve the file locally
	download_file(input_data, local_file)
	
# Back Market Data Engineering Serve team

### Data Pipeline Assessment
You can develop & refactor your code (using your versioning tool) following this pipeline:
1. Import the following file from S3 to GCP:  https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv (Should we mention the file format?)
2. Make the data available in BigQuery for the feature team A which wants only rows for product with an image
3. Make the data available in BigQuery for the feature team B which wants to understand which Samsung products released after 2000 have no images 

>The bm.py retrieves the data from the s3 link and makes the data available in BigQuery through 3 tables : raw & two specific tables for feature teams*

Now Back Market is growing so fast, there are lot more data to import into GCP. How would you adapt your code to scale it up to handle the increasing amount?  

>The bm.py imports data with this unique file as a full dataset, erasing the previous one. It works with a small volumetry but with higher amounts of data, it should be imported incrementally.
>I've added two files in the repository to be used as an example. It should be seen either as an extract of a database made by an cron 
>(for example by getting data from an operational database / raw table with a filter on a created_at/updated_at field in the table to retrieve latest products added/updated) or a file made available by a data producer.

>The bm_incremental.py script is designed to retrieve a data file with a specific date (the extraction part from the source is omitted, let's say it's already there) to insert it in a BigQuery partition.

Eventually, more and more feature teams want to expose data and we want to provide a standard way of doing so. How would you build a tooling library and make it available to them (deployment, versioning, security, legal, etc.)?


### Tips:
1. Do not reinvent the wheel
2. Do not hesitate to make assumptions and to share them
3. Think about the quality of the code
4. Handle the common errors, what if we start again your code?
5. Take the time you need
6. Enjoy!
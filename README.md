# Back Market Data Engineering Serve team Interview

### Data Pipeline Assessment
You can develop & refactor your code (using your versioning tool) following this pipeline:
1. Import the following file from S3 to GCP:  https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv (Should we mention the file format?)
2. Make the data available in BigQuery for the feature team A which wants only rows for product with an image
3. Make the data available in BigQuery for the feature team B which wants to understand which Samsung products released after 2000 have no images 

>The bm.py retrieves the data from the s3 link and makes the data available in BigQuery through 3 tables : raw & two specific tables for feature teams
>To test : change the project_id in the script, and the schema if the configuration dictionary if needed. It's assumed the credentials are setup in the environment

Now Back Market is growing so fast, there are lot more data to import into GCP. How would you adapt your code to scale it up to handle the increasing amount?  

>The bm.py imports data with this unique file as a full dataset, erasing the previous one. It works with a small volumetry but with higher amounts of data, it should be imported incrementally.
>I've added two files in the repository to be used as an example. It should be seen either as an extract of a database made by an cron 
>(for example by getting data from an operational database / raw table with a filter on a created_at/updated_at field in the table to retrieve latest products added/updated, if it's possible) or a file made available by a data producer.

>The bm_incremental.py script is designed to retrieve a data file with a specific date (the extraction part from the source is omitted, let's say it's already there) to insert it in a BigQuery partition.
>The behaviour used (separate tables per partition and erasing them at loading time) is very Bigquery-centric but it's done with more traditional DWH or even RDBMS by deleting & inserting/updating data to have idempotent pipelines

![Schema](/resources/schema.PNG)

Eventually, more and more feature teams want to expose data and we want to provide a standard way of doing so. How would you build a tooling library and make it available to them (deployment, versioning, security, legal, etc.)?

>A tooling library could be developed by the team responsible of the DWH to make external teams able to push data to the DWH without having to develop specific code and handle access control by themselves.
>The solution will be described as a separate document in the resources directory of this repository

The library could be available in a git repository and pushed by the CI to a pypi index as a versioning package. Every time the DWH team pushes changes on the code, a new tag would be generated 

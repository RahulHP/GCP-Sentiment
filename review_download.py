"""
This script is used to:
1. Download the gzipped JSON files to a Google Compute Engine VM from the source website.
2. Unzip them to a temporary folder on the VM.
3. Upload them to the Google Cloud Storage bucket.

TODO: Check if we need to unzip them. ie. Check if pyspark can read the gzipped json files directly
WISH: Check file length before downloading it and then create VM based on this file size to save costs.
	Currently, one big VM is created which downloads all the files (from small ones to huge ones)

"""

import urllib
import gzip
from google.cloud import storage
from CategoryGenerator import CategoryGenerator

client = storage.Client()
bucket = client.get_bucket('pysenti-data')

categorygenerator = CategoryGenerator(bucket='pysenti-data',file_path='review-urls/reviews.txt')
for category in categorygenerator:
	print(category.name)
	urllib.urlretrieve(category.download_url, filename="/tmp/"+category.name+'.json.gz')
	with gzip.open('/tmp/'+category.name+'.json.gz','rb') as f:
		file_blob = storage.Blob(category.reviews_downloaded_json,bucket)
		file_blob.upload_from_file(f,content_type='application/json')
import urllib
import gzip
from google.cloud import storage
client = storage.Client()
bucket = client.get_bucket('pysenti-data')
src_url_file = bucket.get_blob('review-urls/reviews.txt')
src_url_string = src_url_file.download_as_string()
for src_url in src_url_string.split('\r\n'):
	print src_url

	src_file_name = src_url.split('/')[-1]
	category = src_file_name.split('.')[0]
	print(category)
	urllib.urlretrieve(src_url, filename="/tmp/"+src_file_name)
	with gzip.open('/tmp/'+src_file_name,'rb') as f:
		file_blob = storage.Blob('reviews/'+category+'.json',bucket)
		file_blob.upload_from_file(f,content_type='application/json')

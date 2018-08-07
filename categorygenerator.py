from google.cloud import storage
from category import Category
from google.cloud import storage

def CategoryGenerator(bucket,file_path):
	client = storage.Client()
	bucket = client.get_bucket(bucket)
	src_url_file = bucket.get_blob(file_path)
	src_url_string = src_url_file.download_as_string()
	for category_name in self.src_url_string.split('\r\n'):
		yield Category(category_name)
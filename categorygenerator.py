from gcloud import storage
from category import Category

def CategoryGenerator(bucket,file_path):
	client = storage.Client()
	bucket = client.get_bucket(bucket)
	src_url_file = bucket.get_blob(file_path)
	src_url_string = src_url_file.download_as_string()
	for category_name in src_url_string.decode('utf-8').split('\r\n'):
		yield Category(category_name)

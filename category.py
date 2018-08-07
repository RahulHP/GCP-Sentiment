class Category:
	def __init__(self, name):
		super(Category, self).__init__()
		self.name = name
		self.download_url = 'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_'+name+'_5.json.gz'
		self.reviews_downloaded_json = 'gs://pysenti-data/reviews/downloaded/reviews_'+name+'_5.json'
		self.reviews_processed_parquet = 'gs://pysenti-data/reviews/processed/'+name+'.parquet'
		self.reviews_train_parquet = 'gs://pysenti-data/reviews/train/'+name+'.parquet'
		self.reviews_test_parquet = 'gs://pysenti-data/reviews/test/'+name+'.parquet'

		self.lexicons_raw_folder = 'gs://pysenti-data/lexicons/raw/'+name
		self.lexicons_csv = 'gs://pysenti-data/lexicons/processed/'+name+'.csv'

	def __str__(self):
		return self.name


# coding: utf-8

# In[1]:


from gcloud import storage
from pyspark.sql.functions import udf
import re


# In[2]:


client = storage.Client()
bucket = client.get_bucket('pysenti-data')
src_url_file = bucket.get_blob('review-urls/reviews.txt')
src_url_string = src_url_file.download_as_string()


# In[3]:


for src_url in src_url_string.decode('utf-8').split():
    src_file_name = src_url.split('/')[-1]
    category = src_file_name.split('.')[0]
    gs_json_path = 'gs://pysenti-data/reviews/'+category+'.json'
    print(gs_json_path)
    amazon = spark.read.json(gs_json_path)
    amazon = amazon.filter(amazon.overall!=3)
    amazon = amazon.select('asin','reviewerID','reviewText','overall')
    def removePunctuation(text):
        return re.sub('[^a-z ]', " ", text.lower().strip())
    puncUDF = udf(removePunctuation)
    amazon=amazon.withColumn('cleanedReview',puncUDF('reviewText')).drop('reviewText')
    amazon.write.parquet("gs://pysenti-data/reviews-processed/"+category+".parquet")


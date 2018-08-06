
# coding: utf-8

# In[7]:


from gcloud import storage
from pyspark.sql.functions import explode,split


# In[1]:


import sys
get_ipython().system('conda install --yes --prefix {sys.prefix} pandas')


# In[8]:


def generate_lexicon(gs_parquet_file):
	category = gs_parquet_file.split('/')[-1].split('.')[0]
	amazon = spark.read.parquet(gs_parquet_file)
	wordcounts=amazon.withColumn('review_word',explode(split(amazon.cleanedReview,' '))).drop('cleanedReview').select('review_word').groupBy('review_word').count()

	totalwords = wordcounts.groupby().sum().rdd.map(lambda x: x[0]).collect()[0]
	kdic = wordcounts.count()
	print('Dictionary Size: ',kdic)
	p_w = wordcounts.select('review_word','count',((wordcounts['count']+1)/(totalwords+kdic)).alias('p_w'))
	ratingcounts = amazon.select('overall').groupby('overall').count().toPandas().set_index('overall').T.to_dict('list')

	p_pos = float(ratingcounts[4][0]+ratingcounts[5][0])/(ratingcounts[1][0]+ratingcounts[2][0]+ratingcounts[4][0]+ratingcounts[5][0])
	p_neg = 1 - p_pos
	print('Positive reviews proportion: ', p_pos)
	print('Negative reviews proportion: ', p_neg)
	words = wordcounts.select('review_word')

	posreviews = amazon.filter(amazon.overall>=3).drop('overall').withColumn('review_word',explode(split(amazon.cleanedReview,' '))).drop('cleanedReview').groupBy('review_word').count()
	total_pos_count = posreviews.groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]
	p_w_pos_init=words.join(posreviews,'review_word',how='left').fillna(0)
	p_w_pos = p_w_pos_init.withColumn('p_w_pos',((p_w_pos_init['count']+1)/(total_pos_count+kdic))).drop('count')
	p_pos_w=p_w_pos.join(p_w,'review_word',how='inner')
	p_pos_w=p_pos_w.withColumn('p_pos_w',p_pos*p_pos_w['p_w_pos']/p_pos_w['p_w']).select(['review_word','p_pos_w'])


	negreviews = amazon.filter(amazon.overall<=3).drop('overall').withColumn('review_word',explode(split(amazon.cleanedReview,' '))).drop('cleanedReview').groupBy('review_word').count()
	total_neg_count = negreviews.groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]
	p_w_neg_init=words.join(negreviews,'review_word',how='left').fillna(0)
	p_w_neg = p_w_neg_init.withColumn('p_w_neg',((p_w_neg_init['count']+1)/(total_neg_count+kdic))).drop('count')
	p_neg_w=p_w_neg.join(p_w,'review_word',how='inner')
	p_neg_w=p_neg_w.withColumn('p_neg_w',p_neg*p_neg_w['p_w_neg']/p_neg_w['p_w']).select(['review_word','p_neg_w'])


	score_w = p_pos_w.join(p_neg_w,'review_word')
	score_w=score_w.withColumn('score_w',score_w['p_pos_w']-score_w['p_neg_w']).select(['review_word','score_w'])
	output_csv_folder = 'gs://pysenti-data/lexicon/'+category
	print('Saving csv...')
	score_w.coalesce(1).write.csv(output_csv_folder)


# In[9]:


client = storage.Client()
bucket = client.get_bucket('pysenti-data')
src_url_file = bucket.get_blob('review-urls/reviews.txt')
src_url_string = src_url_file.download_as_string()


# In[10]:


for src_url in src_url_string.decode('utf-8').split():
    src_file_name = src_url.split('/')[-1]
    category = src_file_name.split('.')[0]
    print(category)
    gs_parquet_file = "gs://pysenti-data/reviews-processed/"+category+".parquet"
    generate_lexicon(gs_parquet_file)


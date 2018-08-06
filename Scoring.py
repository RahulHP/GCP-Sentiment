
# coding: utf-8

# In[32]:


from pyspark.sql.functions import concat_ws,split,explode,sum,avg,udf
from pyspark.sql.types import StructType, StringType, DoubleType,BooleanType


# In[2]:


review_category = 'reviews_Baby_5'


# In[6]:


lexicon_category = 'reviews_Baby_5'


# In[4]:


amazon = spark.read.parquet('gs://pysenti-data/reviews-processed/'+review_category+'.parquet')


# In[5]:


amazon


# In[7]:


struct = StructType().add(field="word", data_type=StringType()).add(field="score", data_type=DoubleType())
words = spark.read.csv('gs://pysenti-data/lexicon-saved/'+lexicon_category+'.csv', schema=struct)


# In[40]:


def rating_to_sentiment(rating):
	return rating > 3

ratingUDF = udf(rating_to_sentiment, BooleanType())

def score_to_sentiment(score):
	return score >= 0
scoreUDF = udf(score_to_sentiment, BooleanType())


# In[41]:


amazon = amazon.withColumn('reviewID',concat_ws('-',amazon.asin,amazon.reviewerID))


# In[42]:


exploded_words = amazon.withColumn('review_word',explode(split(amazon.cleanedReview,' '))).drop('cleanedReview')
indi_word_scores = exploded_words.join(words,exploded_words.review_word==words.word,how='inner').select('reviewID','overall','score')


# In[43]:


review_score = indi_word_scores.groupBy('reviewID','overall').agg(sum('score').alias('review_score'))


# In[44]:


final = review_score.withColumn('target',ratingUDF('overall')).withColumn('pred',scoreUDF('review_score')).drop('overall','review_score')


# In[47]:


final.cache()


# In[48]:


tp = final[(final.target) & (final.pred)].count()
tn = final[(~ final.target) & (~ final.pred)].count()
fp = final[(~ final.target) & (final.pred)].count()
fn = final[(final.target) & (~ final.pred)].count()


# In[53]:


recall = float(tp)/(tp + fn)
precision = float(tp) / (tp + fp)
accuracy = float(tp+tn) / (tp+tn+fp+fn)
f1 = 2*precision*recall / (precision+recall)


# In[54]:


f1


# In[72]:


def rating_to_sentiment(rating):
	return rating > 3

ratingUDF = udf(rating_to_sentiment, BooleanType())

def score_to_sentiment(score):
	return score >= 0
scoreUDF = udf(score_to_sentiment, BooleanType())


# In[74]:


type(spark)


# In[92]:


def create_metrics(review_parquet, lexicon_csv):
    amazon = spark.read.parquet(review_parquet)
    struct = StructType().add(field="word", data_type=StringType()).add(field="score", data_type=DoubleType())
    words = spark.read.csv(lexicon_csv, schema=struct)
    amazon = amazon.withColumn('reviewID',concat_ws('-',amazon.asin,amazon.reviewerID))
    exploded_words = amazon.withColumn('review_word',explode(split(amazon.cleanedReview,' '))).drop('cleanedReview')
    indi_word_scores = exploded_words.join(words,exploded_words.review_word==words.word,how='inner').select('reviewID','overall','score')
    review_score = indi_word_scores.groupBy('reviewID','overall').agg(sum('score').alias('review_score'))
    final = review_score.withColumn('target',ratingUDF('overall')).withColumn('pred',scoreUDF('review_score')).drop('overall','review_score')
    
    tp = final[(final.target) & (final.pred)].count()
    tn = final[(~ final.target) & (~ final.pred)].count()
    fp = final[(~ final.target) & (final.pred)].count()
    fn = final[(final.target) & (~ final.pred)].count()
    recall = float(tp)/(tp + fn)
    precision = float(tp) / (tp + fp)
    accuracy = float(tp+tn) / (tp+tn+fp+fn)
    f1 = 2*precision*recall / (precision+recall)
    print(tp,tn,fp,fn)
    print("Accuracy: ",accuracy)
    print("F1 Score: ",f1)
    return {'tp':tp,
           'tn':tn,
           'fp':fp,
           'fn':fn,
           'recall':recall,
           'precision':precision,
           'accuracy':accuracy,
           'f1':f1}


# In[93]:


def test_lexicon(review_category,lexicon_category):
    review_parquet = 'gs://pysenti-data/reviews-processed/'+review_category+'.parquet'
    lexicon_csv = 'gs://pysenti-data/lexicon-saved/'+lexicon_category+'.csv'
    d = create_metrics(review_parquet, lexicon_csv)
    d['review_category'] = review_category
    d['lexicon_category'] = lexicon_category
    return d


# In[94]:


from itertools import product


# In[95]:


from gcloud import storage


# In[96]:


client = storage.Client()
bucket = client.get_bucket('pysenti-data')
src_url_file = bucket.get_blob('review-urls/reviews.txt')
src_url_string = src_url_file.download_as_string()
categories = list()
for src_url in src_url_string.decode('utf-8').split():
    src_file_name = src_url.split('/')[-1]
    category = src_file_name.split('.')[0]
    categories.append(category)


# In[97]:


results = list()


# In[ ]:


for review_category, lexicon_category in product(categories,categories):
    print(review_category+'---'+lexicon_category)
    results.append(test_lexicon(review_category,lexicon_category))


# In[99]:


results


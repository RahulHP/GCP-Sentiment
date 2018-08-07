from gcloud import storage
from pyspark.sql.functions import explode,split

# TODO: Remove this step either by using the datalab initialisation script or installing pandas when the VM is spun up.
import sys
get_ipython().system('conda install --yes --prefix {sys.prefix} pandas')


def generate_lexicon(category):
	amazon = spark.read.parquet(category.reviews_train_parquet).select('cleanedReview','target')

	sentiment_level_per_word_counts = amazon.withColumn('review_word', explode(split( amazon.cleanedReview,' '))).drop('cleanedReview').groupBy('target','review_word').count()
	overall_per_word_counts = wordcounts.groupBy('review_word').agg(sum('count').alias('count'))

	total_words_count = overall_per_word_counts.groupby().sum().rdd.map(lambda x: x[0]).collect()[0]
	lexicon_size = total_words_count.count()
	print('Lexicon Size: ',lexicon_size)
	p_w = overall_per_word_counts.select('review_word','count',((overall_per_word_counts['count']+1)/(total_words_count+lexicon_size)).alias('p_w'))
	words = overall_per_word_counts.select('review_word')

	# TODO: Remove pandas requirement if possible
	sentiment_counts = amazon.select('target').groupby('target').count().toPandas().set_index('target').T.to_dict('list')
	p_pos = float(sentiment_counts[True][0])/(sentiment_counts[True][0]+sentiment_counts[False][0])
	p_neg = 1 - p_pos
	print('Positive reviews proportion: ', p_pos)
	print('Negative reviews proportion: ', p_neg)
	
	# Find p(positive|word) = p(positive & word) / p(word) = p(positive) * p(word|positive) / p(word)
	pos_review_word_counts = sentiment_level_per_word_counts.filter(sentiment_level_per_word_counts.target).drop('target')
	positive_words_count = pos_review_word_counts.groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]
	p_w_pos_init = words.join(pos_review_word_counts,'review_word',how='left').fillna(0)
	p_w_pos = p_w_pos_init.withColumn('p_w_pos',((p_w_pos_init['count']+1)/(positive_words_count+lexicon_size))).drop('count')
	p_pos_w = p_w_pos.join(p_w,'review_word',how='inner')
	p_pos_w = p_pos_w.withColumn('p_pos_w',p_pos*p_pos_w['p_w_pos']/p_pos_w['p_w']).select(['review_word','p_pos_w'])

	# Find p(negative|word) = p(negative & word) / p(word) = p(negative) * p(word|negative) / p(word)
	neg_review_word_counts = sentiment_level_per_word_counts.filter(~sentiment_level_per_word_counts.target).drop('target')
	negative_words_count = neg_review_word_counts.groupBy().sum().rdd.map(lambda x: x[0]).collect()[0]
	p_w_neg_init = words.join(neg_review_word_counts,'review_word',how='left').fillna(0)
	p_w_neg = p_w_neg_init.withColumn('p_w_neg',((p_w_neg_init['count']+1)/(negative_words_count+lexicon_size))).drop('count')
	p_neg_w = p_w_neg.join(p_w,'review_word',how='inner')
	p_neg_w = p_neg_w.withColumn('p_neg_w',p_neg*p_neg_w['p_w_neg']/p_neg_w['p_w']).select(['review_word','p_neg_w'])

	# Find score(w) = p(positive|word) - p(negative|word)
	score_w = p_pos_w.join(p_neg_w,'review_word')
	score_w = score_w.withColumn('score_w',score_w['p_pos_w']-score_w['p_neg_w']).select(['review_word','score_w'])
	score_w.coalesce(1).write.csv(category.lexicons_raw_folder)

	# TODO: Add code to move/rename created csv file to correct path category.lexicons_csv


categorygenerator = CategoryGenerator(bucket='pysenti-data',file_path='review-urls/reviews.txt')
for category in categorygenerator:
	print(category.name)
	generate_lexicon(category)

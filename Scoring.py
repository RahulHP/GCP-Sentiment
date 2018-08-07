"""
This script is used to:
1. Generate predictions for a set of reviews using a given lexicon.
2. Find the following metrics given the true values and predictions : Accuracy, Precision, Recall, F1 Score
3. Store the values for all combinations in a CSV.
"""
from itertools import product
from gcloud import storage
from pyspark.sql.functions import concat_ws,split,explode,sum,avg,udf
from pyspark.sql.types import StructType, StringType, DoubleType,BooleanType


def score_to_sentiment(score):
    return score >= 0


scoreUDF = udf(score_to_sentiment, BooleanType())


def generate_predictions(review_parquet, lexicon_csv):
    # TODO: Check if columns exist
    amazon = spark.read.parquet(review_parquet)

    # TODO: Change lexicon structure so that 'review_word' is used instead of 'word'
    struct = StructType().add(field="word", data_type=StringType()).add(field="score", data_type=DoubleType())
    words = spark.read.csv(lexicon_csv, schema=struct)

    # TODO: Should we create reviewID while processing it (instead of here) to be on the safer side?
    amazon = amazon.withColumn('reviewID', concat_ws('-',amazon.asin,amazon.reviewerID))
    exploded_words = amazon.withColumn('review_word', explode(split(amazon.cleanedReview, ' '))).drop('cleanedReview')
    indi_word_scores = exploded_words.join(words, exploded_words.review_word == words.word, how='inner').select('reviewID', 'overall', 'score')
    review_score = indi_word_scores.groupBy('reviewID', 'overall').agg(sum('score').alias('review_score'))
    final = review_score.withColumn('pred', scoreUDF('review_score')).drop('overall', 'review_score')
    return final


def create_metrics(dataframe):
    # TODO: Check if columns exist
    tp = dataframe[(dataframe.target) & (dataframe.pred)].count()
    tn = dataframe[(~ dataframe.target) & (~ dataframe.pred)].count()
    fp = dataframe[(~ dataframe.target) & (dataframe.pred)].count()
    fn = dataframe[(dataframe.target) & (~ dataframe.pred)].count()
    recall = float(tp)/(tp + fn)
    precision = float(tp) / (tp + fp)
    accuracy = float(tp+tn) / (tp+tn+fp+fn)
    f1 = 2*precision*recall / (precision+recall)
    print(tp, tn, fp, fn)
    print("Accuracy: ", accuracy)
    print("F1 Score: ", f1)
    return {'tp': tp,
            'tn': tn,
            'fp': fp,
            'fn': fn,
            'recall': recall,
            'precision': precision,
            'accuracy': accuracy,
            'f1': f1}


def test_lexicon(review_category, lexicon_category, train):
    review_parquet = category.reviews_train_parquet if train else category.reviews_test_parquet
    lexicon_csv = lexicon_category.lexicons_csv
    predictions = generate_predictions(review_parquet, lexicon_csv)
    d = create_metrics(predictions)
    d['review_set'] = 'train' if train else 'test'
    d['review_category'] = review_category.name
    d['lexicon_category'] = lexicon_category.name
    return d


client = storage.Client()
bucket = client.get_bucket('pysenti-data')
src_url_file = bucket.get_blob('review-urls/reviews.txt')
src_url_string = src_url_file.download_as_string()
categories = list()
for src_url in src_url_string.decode('utf-8').split():
    src_file_name = src_url.split('/')[-1]
    category = src_file_name.split('.')[0]
    categories.append(category)


results = list()
for review_category, lexicon_category in product(categories, categories):
    print(review_category.name+'---'+lexicon_category.name)
    results.append(test_lexicon(review_category, lexicon_category, train=True))
    results.append(test_lexicon(review_category, lexicon_category, train=False))


print(results)

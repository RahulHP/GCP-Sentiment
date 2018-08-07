"""
This script is used to:
1. Remove all reviews with a rating of 3.
2. Keep words comprised of only alphabets. Any words containing numbers or other characters are removed.
3. Save the resulting dataframe to a parquet file.
TODO: Convert ratings (1,2,4,5) into sentiment at this stage.
TODO: Create reviewID = asin+'-'+'reviewerID' at this stage.
TODO: Split into stratified train-test dataframes.
"""

from pyspark.sql.functions import udf
from CategoryGenerator import CategoryGenerator
import re


def removePunctuation(text):
    return re.sub('[^a-z ]', " ", text.lower().strip())


puncUDF = udf(removePunctuation)

def rating_to_sentiment(rating):
    return rating > 3


ratingUDF = udf(rating_to_sentiment, BooleanType())

categorygenerator = CategoryGenerator(bucket='pysenti-data',file_path='review-urls/reviews.txt')
for category in categorygenerator:
    print(category.name)
    amazon = spark.read.json(category.reviews_downloaded_json)
    amazon = amazon.filter(amazon.overall!=3)
    amazon = amazon.withColumn('cleanedReview',puncUDF('reviewText'))
    amazon = amazon.withColumn('target', ratingUDF('overall'))
    amazon = amazon.select('asin','reviewerID','cleanedReview','target')
    amazon.write.parquet(category.reviews_processed_parquet)

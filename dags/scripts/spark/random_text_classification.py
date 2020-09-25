# pyspark
import argparse

from pyspark.sql import SparkSession
from pyspark.ml.feature import *
from pyspark.sql.functions import *
from pyspark.ml.classification import *
from pyspark.ml.tuning import *
from pyspark.sql.types import *
from pyspark.ml.evaluation import *

from textblob import TextBlob


def random_text_classifier(input_loc, output_loc):
    """
        1. clean input data
        2. use a pre-trained model to make prediction
        3. write predictions to a HDFS output
    """

    # read input
    df_raw = spark.read.csv(input_loc, inferSchema = True, header = True)
    # perform text cleaning
    df_raw = df_raw.withColumn('review_str',
                                translate(col('review_str'), '<.*?>', ' '))\
                .withColumn('review_str',
                            translate(col('review_str'), '(', ' '))\
                .withColumn('review_str',
                            translate(col('review_str'), ')', ' '))
    # remove punctuation
    df_raw = df_raw.withColumn('review_str',
                            regexp_replace(col('review_str'), '[^A-Za-z ]+', ''))

    # remove any long spaces
    df_raw = df_raw.withColumn('review_str',
                        regexp_replace(col('review_str'), ' +', ' '))

    # lowercase all words
    df_raw = df_raw.withColumn('review_str', lower(col('review_str')))

    # label sentiment
    def getSentiment(text):
        sentiment = TextBlob(text).sentiment.polarity
        if sentiment > 0:
            return 'pos'
        else:
            return 'neg'

    sentiment = udf(lambda x: getSentiment(x), StringType())
    df_raw = df_raw.withColumn('pos', sentiment(col('review_str')))

    # Tokenize text
    tokenizer = Tokenizer(inputCol='review_str', outputCol='review_token')
    df_tokens = tokenizer.transform(df_raw).select('cid', 'review_token', 'pos')

    # Remove stop words
    remover = StopWordsRemover(inputCol='review_token', outputCol='review_clean')
    df_clean = remover.transform(df_tokens).select('cid', 'review_clean', 'pos')

    indexer = StringIndex(inputCol = 'pos', outputCol = 'label')
    df = indexer.fit(df_clean).transform(df_clean)

    # convert text to vector to train model
    # here we use word2vec as example
    word2Vec = Word2Vec(vectorSize = 3, minCount = 0, inputCol = 'review_clean', outputCol = 'features')
    df_after = word2Vec.fit(df).transform(df)

    classifier = RandomForestClassifier()
    paramGrid = (ParamGridBuilder()\
                .addGrid(classifier.maxDepth, [5, 10, 15])\
                .build())
    cv = CrossValidator(estimator = classifier,
                        estimatorParamMaps = paramGrid,
                        evalutor = BinaryClassificationEvaluator(rawPredictionCol = 'prediction'),
                        numFolds = 10)
    res = cv.fit(df_after).transform(df_after)
    df_out = res.withColumn('class', when(col('output') == 1.0, 'positive').otherwise('negative'))\
                        .select('cid', 'class')

    # parquet is a popular column storage format, we use it here
    df_out.write.mode("overwrite").parquet(output_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', type=str,
                        help='HDFS input', default='/movie')
    parser.add_argument('--output', type=str,
                        help='HDFS output', default='/output')
    args = parser.parse_args()
    spark = SparkSession.builder.appName('Random Text Classifier').getOrCreate()
    random_text_classifier(input_loc=args.input, output_loc=args.output)

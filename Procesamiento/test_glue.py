import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import datetime, timedelta
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, CountVectorizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
import spacy
  
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
S3_RAW_PATH = "s3://spaceflight/raw/"
S3_OUT_TOPIC_PATH = "s3://spaceflight/curada/topic/"
S3_OUT_SITE_PATH = "s3://spaceflight/curada/site/"
S3_OUT_FACT_PATH = "s3://spaceflight/curada/fact/"
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
print(yesterday)
def load_file(spark):
    df_article = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").load(S3_RAW_PATH + "Article_20250203.csv")
    df_article = df_article.withColumn("name_orige", lit("Article"))
    df_blog = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").load(S3_RAW_PATH + "Blog_20250203.csv")
    df_blog = df_blog.withColumn("name_orige", lit("Blog"))
    df_Report = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").load(S3_RAW_PATH + "Report_20250203.csv")
    df_Report = df_Report.withColumn("name_orige", lit("Report"))

    df_all = df_article.union(df_blog).union(df_Report).distinct()
    df_all = df_all.withColumn("published_at", to_timestamp("published_at")).withColumn("updated_at", to_timestamp("updated_at"))
    df_all = df_all.withColumn("year", year("published_at")).withColumn("month", month("published_at")).withColumn("hour", hour("published_at"))
    return df_all
def main_site(df):
    df = df.withColumn("base_url", regexp_extract(df["url"], r"(https?://[^/]+)", 1))
    df_by_site = df.groupBy("news_site", "base_url").count().orderBy(desc("count"))
    total = df_all.count()
    window = Window.orderBy("news_site")
    df_by_site = df_by_site.withColumn("sourced_id", row_number().over(window)).withColumn("percentage", round((col("count") / total) * 100, 2)).withColumn("load_date", date_format(current_date(), "yyyyMMdd"))
    df_by_site = df_by_site.select(col("sourced_id"), col("news_site").alias("name"), col("base_url").alias("url"), col("percentage").alias("reliability_score"), col("load_date"))
    return df_by_site
def main_topic_extraction(df):
    nlp = spacy.load("en_core_web_sm")
    def extract_entities(text):
        if text is None:
            return []
        doc = nlp(text)
        entities = [ent.text for ent in doc.ents if ent.label_ in ["ORG", "PERSON", "GPE"]]
        return list(set(entities))  

    entity_udf = udf(extract_entities, ArrayType(StringType()))
    df_entities = df.withColumn("entities", entity_udf(col("title")))
    df_entities = df_entities.select(col("id").alias("id_articule"), "entities")
    return df_entities
def fact_article(df, df_site, df_topic):
    df_fact = df.join(df_site, df.news_site == df_site.name, "left").join(df_topic, df.id == df_topic.id_articule, 'left')
    df_fact = df_fact.withColumn("load_date_fact", date_format(current_date(), "yyyyMMdd"))
    df_fact = df_fact.select(col("id").alias("article_id"), col("sourced_id") , col("entities"), col("published_at"), col('title'), col('summary'), col("load_date_fact").alias("load_date") ) 
    return df_fact

df_all = load_file(spark)
df_site = main_site(df_all)
df_site.write.mode("overwrite").format("parquet").partitionBy("load_date").option("path", S3_OUT_SITE_PATH).save()
topic = main_topic_extraction(df_all)
topic.write.mode("overwrite").format("parquet").partitionBy("load_date").option("path", S3_OUT_TOPIC_PATH ).save()
fact = fact_article(df_all, df_site, topic)
fact.write.mode("overwrite").format("parquet").partitionBy("load_date").option("path", S3_OUT_FACT_PATH ).save()

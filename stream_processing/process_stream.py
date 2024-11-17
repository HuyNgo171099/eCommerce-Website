'''
PART I: IMPORTING LIBRARIES
'''

# Spark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# MongoDB libraries
from pymongo import MongoClient

# general libraries
import os
import logging

'''
PART II: SPARK AND KAFKA BORKERS SETUP
'''

# initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# function to initialize Spark session with MongoDB and Kafka configurations
def create_spark_connection():
    spark = None
    try:
        # MongoDB settings
        mongo_host = "mongodb"  
        mongo_port = "27017"  
        mongo_database = "ecommerce_website" 
        mongo_username = "<your_user_name>" 
        mongo_password = "<your_password>" 

        # MongoDB base connection URI
        mongo_base_uri = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_database}?authSource=admin"

        # Initialize Spark session in local mode without Ivy or Kerberos
        spark = SparkSession.builder \
            .appName("KafkaMongoDBStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
            .config("spark.mongodb.read.connection.uri", mongo_base_uri) \
            .config("spark.mongodb.write.connection.uri", mongo_base_uri) \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session created successfully in local mode.")
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
    
    return spark

# specify Kafka bootstrap servers
kafka_bootstrap_servers = "kafka1:29092,kafka2:29094"

'''
PART III: FUNCTIONS TO READ DATA FROM KAFKA
'''

# define a function to read customer streams from Kafka
def kafka_customer_streams(spark_conn):
    customerSchema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("location", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("account_created", StringType(), True),
        StructField("last_login", TimestampType(), True)
    ])
    customerDF = None
    try:
        customerDF = (spark_conn.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                      .option("subscribe", "customers")
                      .option("startingOffsets", "earliest")
                      .load()
                      .selectExpr("CAST(value AS STRING)")
                      .select(from_json("value", customerSchema).alias("data"))
                      .select("data.*")
                      .withWatermark("last_login", "2 hours"))
        logger.info("Spark customer dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating customer dataframe: {e}")
    return customerDF   

# define a function to read product streams from Kafka
def kafka_product_streams(spark_conn):
    productSchema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("supplier", StringType(), True),
        StructField("rating", DoubleType(), True)
    ])
    productDF = None
    try:
        productDF = (spark_conn.readStream
                     .format("kafka")
                     .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                     .option("subscribe", "products")
                     .option("startingOffsets", "earliest")
                     .load()
                     .selectExpr("CAST(value AS STRING)")
                     .select(from_json("value", productSchema).alias("data"))
                     .select("data.*")
                     .withColumn("processingTime", current_timestamp())
                     .withWatermark("processingTime", "2 hours"))
        logger.info("Spark product dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating product dataframe: {e}")
    return productDF

# define a function to read transaction streams from Kafka
def kafka_transaction_streams(spark_conn):
    transactionSchema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("date_time", TimestampType(), True),  
        StructField("status", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    transactionDF = None
    try:
        transactionDF = (spark_conn.readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                         .option("subscribe", "transactions")
                         .option("startingOffsets", "earliest")
                         .load()
                         .selectExpr("CAST(value AS STRING)")
                         .select(from_json("value", transactionSchema).alias("data"))
                         .select("data.*")
                         .withColumn("processingTime", current_timestamp())
                         .withWatermark("processingTime", "2 hours"))
        logger.info("Spark transaction dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating transaction dataframe: {e}") 
    return transactionDF

# define a function to read product_views streams from Kafka
def kafka_product_views_streams(spark_conn):
    productViewSchema = StructType([
        StructField("view_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),  
        StructField("view_duration", IntegerType(), True)
    ])
    productViewDF = None
    try:
        productViewDF = (spark_conn.readStream
                         .format("kafka")
                         .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                         .option("subscribe", "product_views")
                         .option("startingOffsets", "earliest")
                         .load()
                         .selectExpr("CAST(value AS STRING)")
                         .select(from_json("value", productViewSchema).alias("data"))
                         .select("data.*")
                         .withColumn("timestamp", col("timestamp").cast("timestamp"))
                         .withWatermark("timestamp", "1 hour"))
        logger.info("Spark product views dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating product view dataframe: {e}")    
    return productViewDF

# define a function to read system logs streams from Kafka
def kafka_system_logs_streams(spark_conn):
    systemLogSchema = StructType([
        StructField("log_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),  
        StructField("level", StringType(), True),
        StructField("message", StringType(), True)
    ])
    systemLogDF = None
    try:
        systemLogDF = (spark_conn.readStream
                       .format("kafka")
                       .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                       .option("subscribe", "system_logs")
                       .option("startingOffsets", "earliest")
                       .load()
                       .selectExpr("CAST(value AS STRING)")
                       .select(from_json("value", systemLogSchema).alias("data"))
                       .select("data.*")
                       .withColumn("processingTime", current_timestamp())
                       .withWatermark("processingTime", "2 hours"))
        logger.info("Spark system logs dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating system log dataframe: {e}")
    return systemLogDF

# define a function to read user interactions streams from Kafka
def kafka_user_interactions_streams(spark_conn):
    userInteractionSchema = StructType([
        StructField("interaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),  
        StructField("interaction_type", StringType(), True),
        StructField("details", StringType(), True)
    ])
    userInteractionDF = None
    try:
        userInteractionDF = (spark_conn.readStream
                             .format("kafka")
                             .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                             .option("subscribe", "user_interactions")
                             .option("startingOffsets", "earliest")
                             .load()
                             .selectExpr("CAST(value AS STRING)")
                             .select(from_json("value", userInteractionSchema).alias("data"))
                             .select("data.*")
                             .withColumn("processingTime", current_timestamp())
                             .withWatermark("processingTime", "2 hours"))
        logger.info("Spark user interactions dataframe created successfully")
    except Exception as e:
        logger.error(f"Error creating user interaction dataframe: {e}") 
    return userInteractionDF

'''
PART IV: FUNCTIONS TO ANALYZE DATA
'''

# function to analyze customer data
def analyze_customer_data(customerDF):
    customerAnalysisDF = (customerDF
                          .groupBy(window(col("last_login"), "1 day"), "gender")
                          .agg(count("customer_id").alias("total_customers"),
                               max("last_login").alias("last_activity")))
    return customerAnalysisDF

# function to analyze product data
def analyze_product_data(productDF):
    productAnalysisDF = (productDF
                         .groupBy(window(col("processingTime"), "1 hour"), "category")
                         .agg(avg("price").alias("average_price"),
                              sum("stock_quantity").alias("total_stock"))
                         .select(col("window.start").alias("window_start"),
                                 col("window.end").alias("window_end"),
                                 col("category"),
                                 col("average_price"),
                                 col("total_stock")))
    return productAnalysisDF

# define a function to analyze transaction data
def analyze_transaction_data(transactionDF):
    salesAnalysisDF = (transactionDF
                       .groupBy(window(col("processingTime"), "1 hour"), "product_id")
                       .agg(count("transaction_id").alias("number_of_sales"),
                            sum("quantity").alias("total_quantity_sold"),
                            approx_count_distinct("customer_id").alias("unique_customers"))
                       .select(col("window.start").alias("window_start"),
                               col("window.end").alias("window_end"),
                               col("product_id"),
                               col("number_of_sales"),
                               col("total_quantity_sold"),
                               col("unique_customers")))
    return salesAnalysisDF

# define a function to analyze product views data
def analyze_product_views_data(productViewDF):
    productViewsAnalysisDF = (productViewDF
                              .groupBy(window(col("timestamp"), "1 hour"), "product_id")
                              .agg(count("view_id").alias("total_views"),
                                   avg("view_duration").alias("average_view_duration"))
                              .select(col("window.start").alias("window_start"),
                                      col("window.end").alias("window_end"),
                                      col("product_id"),
                                      col("total_views"),
                                      col("average_view_duration")))
    return productViewsAnalysisDF

'''
PART V: FUNCTIONS TO EXPORT DATA TO MONGODB
'''
        
def export_to_mongodb(dataframe, collection_name, output_mode, mode):
    try:
        export_stream = (dataframe.writeStream
                         .outputMode(output_mode)
                         .foreachBatch(lambda batch_df, batch_id: batch_df.write
                                       .format("mongodb")
                                       .mode(mode)
                                       .option("spark.mongodb.connection.uri", f"mongodb://<your_user_name>:<your_password>@mongodb:27017/ecommerce_website?authSource=admin")
                                       .option("spark.mongodb.database", "ecommerce_website")
                                       .option("spark.mongodb.collection", collection_name)
                                       .save())
                         .start())
        export_stream.awaitTermination(30)
        logger.info(f"Data exported to MongoDB collection: {collection_name}")
    except Exception as e:
        logger.error(f"Error exporting data to MongoDB collection {collection_name}: {e}")
      
'''
PART VI: EXECUTE ALL THE FUNCTIONS
'''
spark_conn = create_spark_connection()

if spark_conn is not None:
    customerDF = kafka_customer_streams(spark_conn)
    productDF = kafka_product_streams(spark_conn)
    transactionDF = kafka_transaction_streams(spark_conn)
    productViewDF = kafka_product_views_streams(spark_conn)

    customerAnalysisDF = analyze_customer_data(customerDF)
    productAnalysisDF = analyze_product_data(productDF)
    salesAnalysisDF = analyze_transaction_data(transactionDF)
    productViewsAnalysisDF = analyze_product_views_data(productViewDF)

    export_to_mongodb(customerAnalysisDF, "customer_analysis", "append", "append")
    export_to_mongodb(productAnalysisDF, "product_analysis", "complete", "overwrite")
    export_to_mongodb(salesAnalysisDF, "sales_analysis", "complete", "overwrite")
    export_to_mongodb(productViewsAnalysisDF, "product_views_analysis", "append", "append")


    
    
      
            
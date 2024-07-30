import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import sum as _sum


if __name__ == "__main__":
    # print(pyspark.__version__)
    # Initializing spark session
    spark = (SparkSession.builder
             .appName("RealTimeEngineering")
             .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1')     # spark kafka integration
             .config('spark.jars', 'C:\\Users\\AL KAREEM\\Desktop\\Rais\\RealTimeProjects\\RealTimeVotingDataEngineering\\postgresql-42.7.3.jar')     # postgresql driver
             .config('spark.sql.adaptive.enable', 'false') # disable adaptive query execution
             .getOrCreate())
    
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", StringType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("compaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),           
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("voter_id", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True),
    ])

    votes_df = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'localhost:9092')
                .option('subscribe', 'votes_topic')
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), vote_schema).alias("data"))
                .select('data.*')
                )
    
    # Data preprocessing Converting the voting timestamp into timestamp
    votes_df = votes_df.withColumn("voting_time", col('voting_time').cast(TimestampType()))\
                        .withColumn("vote", col("vote").cast(IntegerType()))
    
    # Watermarking
    enriched_votes_df = votes_df.withWatermark("voting_time", '1 minute')

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy('candidate_id', 'candidate_name', 'party_affiliation', 
                                                    'photo_url').agg(_sum('vote').alias("total_votes"))
    
    turnout_by_locaiton = enriched_votes_df.groupBy('address.state').count().alias("total_votes")

    votes_per_candidate_to_kafka = (
                                    votes_per_candidate.selectExpr("to_json(struct(*)) AS value")
                                    .writeStream
                                    .format("kafka")
                                    .option("kafka.bootstrap.server", 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocatoin', 'C:\\Users\\AL KAREEM\\Desktop\\Rais\\RealTimeProjects\\RealTimeVotingDataEngineering\\checkpoints\\checkpoint1')
                                    .mode("update")
                                    .start()
                                )
    
    turnout_by_locaiton_to_kafka = (
                                    turnout_by_locaiton.selectExpr("to_json(struct(*)) AS value")
                                    .writeStream
                                    .format("kafka")
                                    .option("kafka.bootstrap.server", 'localhost:9092')
                                    .option('topic', 'aggregated_turnout_by_locatoin')
                                    .option('checkpointLocatoin', 'C:\\Users\\AL KAREEM\\Desktop\\Rais\\RealTimeProjects\\RealTimeVotingDataEngineering\\checkpoints\\checkpoint2')
                                    .mode("update")
                                    .start()
                                )
    
    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_locaiton_to_kafka.awaitTermination()
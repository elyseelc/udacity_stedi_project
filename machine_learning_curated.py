# Python script that creates an aggregated table and populates machine_learning_curated

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize the GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('machine_learning_curated', args)

# Provide your database and tables names
database = "stediproject_database2"
step_trainer_table = "step_trainer_trusted"
accelerometer_table = "accelerometer_trusted"

# Creating DataFrames from Glue Catalog
step_trainer_df = glueContext.create_dynamic_frame.from_catalog(database=stediproject_database2, table_name=step_trainer_table).toDF()
accelerometer_df = glueContext.create_dynamic_frame.from_catalog(database=stediproject_database2, table_name=accelerometer_table).toDF()

# Join the two datasets
joined_df = step_trainer_df.join(accelerometer_df, on=["timestamp", "customer_id"], how="inner")

# Data Aggregation
aggregated_df = joined_df.groupBy("customer_id", "timestamp").agg({"step_count": "sum", "accelerometer_reading": "avg"})

# Write the aggregated dataframe to a new Glue table in the Curated Zone
glueContext.write_dynamic_frame.from_options(frame=DynamicFrame.fromDF(aggregated_df, glueContext, "aggregated_df"), connection_type="s3", connection_options={"path": "s3://stediprojectbucket-customerlanding1/machine_learning_curated"}, format="parquet", transformation_ctx="datasink2")
job.commit()


Python script that sanitises customer_trusted and creates the Glue table customers_curated

# This script should be okay but will need to be re-run once customers_trusted has been sorted & fixed

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize the GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init('customers_curated_gluejob', {})

# Create dynamic dataframes
df_customers = glueContext.create_dynamic_frame.from_catalog(database="stediproject_database2", table_name="stediprojectbucket_customerlanding1")
df_accelerometer = glueContext.create_dynamic_frame.from_catalog(database="stediproject_database2", table_name="stediproject_accelerometerlanding")

# Convert to Spark DataFrame and join on the user column
df_customers = df_customers.toDF()
df_accelerometer = df_accelerometer.toDF()

# Adjust the join condition according to your data structure
df_customers_curated = df_customers.join(df_accelerometer, df_customers.email == df_accelerometer.user, 'inner')

# Convert back to dynamic dataframe and write the result to S3
df_customers_curated = DynamicFrame.fromDF(df_customers_curated, glueContext, "curated")
glueContext.write_dynamic_frame.from_options(frame = df_customers_curated, connection_type = "s3", connection_options = {"path": "s3://stediprojectbucket-customerlanding1/customers_curated"}, format = "parquet")

# Python script that sanitises accelerometer data creating accelerometer_trusted

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize the GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('sanitise_accelerometer_data_gluejob', {})

# Create dynamic dataframes
df_accelerometer = glueContext.create_dynamic_frame.from_catalog(database="stediproject_database2", table_name="stediproject_accelerometerlanding")
df_customers = glueContext.create_dynamic_frame.from_catalog(database="stediproject_database2", table_name="stediprojectbucket_customerlanding1")

# Convert to Spark DataFrame and join on the user column
df_accelerometer = df_accelerometer.toDF()
df_customers = df_customers.toDF()

# Adjust the join condition according to your data structure
df_accelerometer_trusted = df_accelerometer.join(df_customers, df_accelerometer.user == df_customers.email, 'inner')

# Write the result to S3 in one-line JSON format
df_accelerometer_trusted.write.mode('overwrite').json("s3://stediproject-accelerometerlanding/accelerometer_trusted")



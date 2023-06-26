import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "stediproject_database2", table_name = "customer_landing")

PrivacyFilter_node2 = Filter.apply(
frame=datasource0,
f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
transformation_ctx="PrivacyFilter_node2",
)

glueContext.write_dynamic_frame.from_options(PrivacyFilter_node2, connection_type = "s3", connection_options = {"path": "s3://stediprojectbucket-customerlanding1/customer_trusted"}, format = "parquet", transformation_ctx = "datasink2")

job.commit()

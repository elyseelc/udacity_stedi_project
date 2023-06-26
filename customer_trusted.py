#Python script that sanitises customer data, creating customer_trusted
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
from awsglue.utils import getResolvedOptions

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = logging.getLogger(args['JOB_NAME'])
logger.setLevel(logging.INFO)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database="stediproject_database2", table_name="stediprojectbucket_customerlanding1")

# Print sample data from the shareWithResearchAsOfDate column
datasource0.toDF().select("shareWithResearchAsOfDate").show(10)

# Print total rows before filter
logger.info("This is a print statement Total rows before filter: {}".format(datasource0.count()))

PrivacyFilter_node2 = Filter.apply(
    frame=datasource0,
    f=lambda row: (row["shareWithResearchAsOfDate"] != ""),
    transformation_ctx="PrivacyFilter_node2"
)

# Print total rows after filter
logger.info("This is a print statement Total rows after filter: {}".format(PrivacyFilter_node2.count()))

glueContext.write_dynamic_frame.from_options(PrivacyFilter_node2, connection_type="s3", connection_options={"path": "s3://stediprojectbucket-customerlanding1/customer_trusted"}, format="parquet", transformation_ctx="datasink2")

job.commit()

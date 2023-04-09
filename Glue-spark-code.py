import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680965050538 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-stats-bucket-crawler-cleaned-db",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1680965050538",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680965021023 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-stats-bucket-crawler-cleaned-db",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1680965021023",
)

# Script generated for node Join
Join_node1680965089633 = Join.apply(
    frame1=AWSGlueDataCatalog_node1680965050538,
    frame2=AWSGlueDataCatalog_node1680965021023,
    keys1=["category_id"],
    keys2=["id"],
    transformation_ctx="Join_node1680965089633",
)

# Script generated for node Amazon S3
AmazonS3_node1680965274531 = glueContext.getSink(
    path="s3://youtube-stats-bucket-useast1-analytics-dev",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1680965274531",
)
AmazonS3_node1680965274531.setCatalogInfo(
    catalogDatabase="youtube-stats-analytics-db", catalogTableName="final_analytics"
)
AmazonS3_node1680965274531.setFormat("glueparquet")
AmazonS3_node1680965274531.writeFrame(Join_node1680965089633)
job.commit()

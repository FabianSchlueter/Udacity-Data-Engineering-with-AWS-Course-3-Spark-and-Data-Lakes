import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1740606391054 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1740606391054")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1740606549648 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1740606549648")

# Script generated for node Join Customer and Accelerometer
SqlQuery2009 = '''
with acc_distinct_emails as (select distinct email from acc)
select cus.* from acc_distinct_emails join cus on acc_distinct_emails.email = cus.email
'''
JoinCustomerandAccelerometer_node1740606584436 = sparkSqlQuery(glueContext, query = SqlQuery2009, mapping = {"acc":AccelerometerTrusted_node1740606549648, "cus":CustomerTrusted_node1740606391054}, transformation_ctx = "JoinCustomerandAccelerometer_node1740606584436")

# Script generated for node customers_curated
EvaluateDataQuality().process_rows(frame=JoinCustomerandAccelerometer_node1740606584436, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740606358104", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customers_curated_node1740606833845 = glueContext.getSink(path="s3://fabians-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1740606833845")
customers_curated_node1740606833845.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customers_curated_node1740606833845.setFormat("json")
customers_curated_node1740606833845.writeFrame(JoinCustomerandAccelerometer_node1740606584436)
job.commit()
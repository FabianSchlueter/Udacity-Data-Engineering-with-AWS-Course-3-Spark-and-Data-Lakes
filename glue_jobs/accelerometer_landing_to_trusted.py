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

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1740232308275 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingNode_node1740232308275")

# Script generated for node Customer Trusted
CustomerTrusted_node1740232430497 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1740232430497")

# Script generated for node Join
SqlQuery1936 = '''
select acc.* from acc join cust on acc.user = cust.email
'''
Join_node1740857751285 = sparkSqlQuery(glueContext, query = SqlQuery1936, mapping = {"acc":AccelerometerLandingNode_node1740232308275, "cust":CustomerTrusted_node1740232430497}, transformation_ctx = "Join_node1740857751285")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1740857751285, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740232160624", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1740232775174 = glueContext.getSink(path="s3://fabians-lakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1740232775174")
AccelerometerTrusted_node1740232775174.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1740232775174.setFormat("json")
AccelerometerTrusted_node1740232775174.writeFrame(Join_node1740857751285)
job.commit()
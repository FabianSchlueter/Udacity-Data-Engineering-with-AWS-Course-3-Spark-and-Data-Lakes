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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1740857161443 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1740857161443")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1740857139782 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1740857139782")

# Script generated for node Join
SqlQuery1793 = '''
select * from acc join step on acc.timestamp = step.sensorreadingtime
'''
Join_node1740857198741 = sparkSqlQuery(glueContext, query = SqlQuery1793, mapping = {"acc":AccelerometerTrusted_node1740857161443, "step":StepTrainerTrusted_node1740857139782}, transformation_ctx = "Join_node1740857198741")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=Join_node1740857198741, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740857133120", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1740857354196 = glueContext.getSink(path="s3://fabians-lakehouse/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1740857354196")
MachineLearningCurated_node1740857354196.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1740857354196.setFormat("json")
MachineLearningCurated_node1740857354196.writeFrame(Join_node1740857198741)
job.commit()
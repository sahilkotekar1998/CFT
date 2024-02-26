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

df1=spark.read.format("csv").load("s3://part1-cleaned-data/run-1708753374952-part-r-00000").limit(100)
df1.write.format("parquet").save("s3://grp-03-simulated-data/file1.parquet")




job.commit()
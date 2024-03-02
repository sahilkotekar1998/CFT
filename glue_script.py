import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1709379655170 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://part1-cleaned-data/run-1708753374952-part-r-00000"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1709379655170",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT
    CUST_ID,
    START_DATE,
    CASE
        WHEN LEAD(DATE_OF_TRANS, 1) OVER (PARTITION BY CUST_ID ORDER BY DATE_OF_TRANS) IS NOT NULL
        THEN LEAD(DATE_OF_TRANS, 1) OVER (PARTITION BY CUST_ID ORDER BY DATE_OF_TRANS)
        ELSE END_DATE
    END AS END_DATE,
    TRANS_ID,
    DATE_OF_TRANS,
    YEAR_OF_TRANS,
    MONTH_OF_TRANS,
    DAY_OF_TRANS,
    EXP_TYPE,
    AMOUNT
FROM Cashflow_Clarity
ORDER BY CUST_ID, DATE_OF_TRANS;
"""
SQLQuery_node1709379691901 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"cashflow_Clarity": AmazonS3_node1709379655170},
    transformation_ctx="SQLQuery_node1709379691901",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1709379733535 = DynamicFrame.fromDF(
    SQLQuery_node1709379691901.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1709379733535",
)

# Script generated for node SQL Query
SqlQuery1 = """
SELECT 
    Cust_id,
    Start_date,
    End_date,
    Trans_id,
    Date_of_trans,
    Year_of_trans,
    Month_of_trans,
    Day_of_trans,
    Exp_type,
    Amount,
    date_format(Date_of_trans, 'EEEE') AS Day_of_week
FROM 
    Cashflow_Clarity;
"""
SQLQuery_node1709379736323 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"Casflow_Clarity": DropDuplicates_node1709379733535},
    transformation_ctx="SQLQuery_node1709379736323",
)

# Script generated for node Amazon S3
AmazonS3_node1709379777561 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1709379736323,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://grp-03-simulated-data", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1709379777561",
)

job.commit()

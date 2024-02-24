import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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

# Script generated for node Relational DB
RelationalDB_node1708751761311 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "transactions1",
        "connectionName": "Jdbc connection",
    },
    transformation_ctx="RelationalDB_node1708751761311",
)

# Script generated for node Amazon S3
AmazonS3_node1708751726181 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://s3-split-data-3/s3-data/part-00000-1977fccb-3c9b-4eb2-a490-ad33aa7a7e7b-c000.csv"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1708751726181",
)

# Script generated for node Union
Union_node1708751933861 = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={
        "source1": AmazonS3_node1708751726181,
        "source2": RelationalDB_node1708751761311,
    },
    transformation_ctx="Union_node1708751933861",
)

# Script generated for node Change Schema
ChangeSchema_node1708751975011 = ApplyMapping.apply(
    frame=Union_node1708751933861,
    mappings=[
        ("cust_id", "string", "cust_id", "string"),
        ("start_date", "string", "start_date", "date"),
        ("end_date", "string", "end_date", "date"),
        ("trans_id", "string", "trans_id", "string"),
        ("date", "string", "date_of_trans", "date"),
        ("year", "string", "year_of_trans", "int"),
        ("month", "string", "month_of_trans", "int"),
        ("day", "string", "day_of_trans", "int"),
        ("exp_type", "string", "exp_type", "string"),
        ("amount", "string", "amount", "double"),
    ],
    transformation_ctx="ChangeSchema_node1708751975011",
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
SQLQuery_node1708752030533 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"Cashflow_Clarity": ChangeSchema_node1708751975011},
    transformation_ctx="SQLQuery_node1708752030533",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708752113896 = DynamicFrame.fromDF(
    SQLQuery_node1708752030533.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708752113896",
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
SQLQuery_node1708752119416 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"Cashflow_Clarity": DropDuplicates_node1708752113896},
    transformation_ctx="SQLQuery_node1708752119416",
)

# Script generated for node Amazon S3
AmazonS3_node1708752169024 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1708752119416,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://cleaned-data-grp3", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1708752169024",
)

job.commit()

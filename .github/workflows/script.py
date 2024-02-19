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

# Script generated for node Amazon S3
AmazonS3_node1707978511074 = glueContext.create_dynamic_frame.from_options(
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
            "s3://s3-data-part1/s3_data/part-00000-d482d066-7ac1-4783-814d-27411ca52535-c000.csv"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1707978511074",
)

# Script generated for node Relational DB
RelationalDB_node1707978301399 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "Cashflow_Clarity",
        "connectionName": "rds_connection",
    },
    transformation_ctx="RelationalDB_node1707978301399",
)

# Script generated for node Change Schema
ChangeSchema_node1707978569883 = ApplyMapping.apply(
    frame=AmazonS3_node1707978511074,
    mappings=[
        ("cust_id", "string", "cust_id", "string"),
        ("start_date", "string", "start_date", "string"),
        ("end_date", "string", "end_date", "string"),
        ("trans_id", "string", "trans_id", "string"),
        ("date", "string", "date_of_trans", "string"),
        ("year", "string", "year", "long"),
        ("month", "string", "month", "long"),
        ("day", "string", "day", "long"),
        ("exp_type", "string", "exp_type", "string"),
        ("amount", "string", "amount", "double"),
    ],
    transformation_ctx="ChangeSchema_node1707978569883",
)

# Script generated for node Union
Union_node1707978598632 = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={
        "source1": RelationalDB_node1707978301399,
        "source2": ChangeSchema_node1707978569883,
    },
    transformation_ctx="Union_node1707978598632",
)

# Script generated for node Change Schema
ChangeSchema_node1707978690780 = ApplyMapping.apply(
    frame=Union_node1707978598632,
    mappings=[
        ("cust_id", "string", "cust_id", "string"),
        ("start_date", "string", "start_date", "string"),
        ("end_date", "string", "end_date", "string"),
        ("trans_id", "string", "trans_id", "string"),
        ("date_of_trans", "string", "date_of_trans", "string"),
        ("year", "long", "year", "long"),
        ("month", "long", "month", "long"),
        ("day", "long", "day", "long"),
        ("exp_type", "string", "exp_type", "string"),
        ("amount", "double", "amount", "double"),
    ],
    transformation_ctx="ChangeSchema_node1707978690780",
)

# Script generated for node SQL Query
SqlQuery1 = """
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
SQLQuery_node1707978763766 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"Cashflow_Clarity": ChangeSchema_node1707978690780},
    transformation_ctx="SQLQuery_node1707978763766",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1707978858797 = DynamicFrame.fromDF(
    SQLQuery_node1707978763766.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1707978858797",
)

# Script generated for node SQL Query
SqlQuery0 = """
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
SQLQuery_node1707979108666 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"Cashflow_Clarity": DropDuplicates_node1707978858797},
    transformation_ctx="SQLQuery_node1707979108666",
)

# Script generated for node Amazon S3
AmazonS3_node1707979552466 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1707979108666,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://cleaned-s3-cleaned-data",
        "partitionKeys": [],
    },
    format_options={"compression": "uncompressed"},
    transformation_ctx="AmazonS3_node1707979552466",
)

job.commit()
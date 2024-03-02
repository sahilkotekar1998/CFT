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
RelationalDB_node1708346115675 = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "Cashflow_Clarity",
        "connectionName": "finale",
    },
    transformation_ctx="RelationalDB_node1708346115675",
)

# Script generated for node Amazon S3
AmazonS3_node1708346432156 = glueContext.create_dynamic_frame.from_options(
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
            "s3://part1-cleaned-data/run-1708753374952-part-r-00000"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1708346432156",
)

# Script generated for node Change Schema
ChangeSchema_node1708346497746 = ApplyMapping.apply(
    frame=AmazonS3_node1708346432156,
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
    transformation_ctx="ChangeSchema_node1708346497746",
)

# Script generated for node Union
Union_node1708346568945 = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={
        "source1": RelationalDB_node1708346115675,
        "source2": ChangeSchema_node1708346497746,
    },
    transformation_ctx="Union_node1708346568945",
)

# Script generated for node Change Schema
ChangeSchema_node1708346657708 = ApplyMapping.apply(
    frame=Union_node1708346568945,
    mappings=[
        ("Cust_Id", "string", "Cust_Id", "string"),
        ("Start_Date", "string", "Start_Date", "string"),
        ("End_date", "string", "End_date", "string"),
        ("Trans_Id", "string", "Trans_Id", "string"),
        ("DATE_OF_TRANS", "string", "DATE_OF_TRANS", "string"),
        ("Year", "long", "Year_of_trans", "long"),
        ("Month", "long", "Month_of_trans", "long"),
        ("Day", "long", "Day_of_trans", "long"),
        ("Exp_Type", "string", "Exp_Type", "string"),
        ("Amount", "double", "Amount", "double"),
    ],
    transformation_ctx="ChangeSchema_node1708346657708",
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
SQLQuery_node1708346887114 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"Cashflow_Clarity": ChangeSchema_node1708346657708},
    transformation_ctx="SQLQuery_node1708346887114",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1708346945282 = DynamicFrame.fromDF(
    SQLQuery_node1708346887114.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1708346945282",
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
SQLQuery_node1708346953868 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"Cashflow_Clarity": DropDuplicates_node1708346945282},
    transformation_ctx="SQLQuery_node1708346953868",
)

# Script generated for node Amazon S3
AmazonS3_node1708347017239 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1708346953868,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "grp-03-simulated-data",
        "partitionKeys": ["Exp_Type", "Year_of_Trans", "Month_of_Trans"],
    },
    transformation_ctx="AmazonS3_node1708347017239",
)

job.commit()
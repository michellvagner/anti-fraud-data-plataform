import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, substring

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "anti-fraud-data-platform"

INPUT_PATH = f"s3://{BUCKET}/landing/transactions/"
OUTPUT_PATH = f"s3://{BUCKET}/raw/transactions/"

print("Lendo arquivos CSV...")

df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(INPUT_PATH)
)

print(f"Total de registros: {df.count()}")

columns_as_string = [
    "TRANSACTION_ID",
    "BANK",
    "CARD_NUMBER",
    "AUTHORIZATION_CODE",
    "ACQUIRER_ID",
    "CURRENCY_CD",
    "TRANSACTION_COUNTRY_CD",
    "MERCHANT_ID",
    "REASON_CODE",
    "POS_NUMBER",
    "MERCHANT_CATEGORY_CODE",
    "PROCESS_CODE",
]

for column_name in columns_as_string:
    df = df.withColumn(
        column_name,
        col(column_name).cast("string")
    )
    
df_partitioned = df.withColumn(
    "TRN_DATE",
    substring("TRN_DT", 1, 10)
)

print("Gravando arquivos em Parquet...")

(
    df_partitioned.write
    .mode("overwrite")
    .partitionBy("TRN_DATE")
    .parquet(OUTPUT_PATH)
)

print("Processamento finalizado!")

job.commit()
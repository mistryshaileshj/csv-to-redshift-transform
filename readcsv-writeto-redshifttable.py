import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import concat_ws, upper
import boto3
import time
from pyspark.sql.functions import month, year, col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import split, col
from pyspark.sql.functions import format_string, col
from pyspark.sql.functions import count

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

def main():
    # Define S3 path of the CSV file
    s3_path = "s3://bucketname/customers.csv"
    
    # Read CSV file into a DynamicFrame
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={"withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [s3_path]},
    )
    
    # Convert DynamicFrame to Spark DataFrame
    df = dynamic_frame.toDF()
    
    # Show schema and sample data
    df.printSchema()
    #df.show()
    
    #read and print row count of the dataset
    dflen = df.count()
    print(f"Dataset length : {dflen}")
    
    #read first row and print
    firstrow = df.first()
    print(f"first row values : {firstrow}")
    
    # Create new column 'fullname' by concatenating 'firstname' and 'lastname'
    df = df.withColumn("fullname", concat_ws(" ", df["First Name"], df["Last Name"]))
    
    # Show updated DataFrame
    # df.select("First Name", "Last Name", "fullname", "Subscription Date").show()
    
    # convert fullname column to upper case
    df = df.withColumn("fullname_upper", upper(df["fullname"]))
    
    # Convert string to date format (adjust format if needed)
    df = df.withColumn("Subscription Date", to_date(col("Subscription Date"), "dd-MM-yyyy"))
    
    #df.select("First Name", "Last Name", "fullname", "fullname_upper", "Subscription Date").show()
    
    # Extract month and year from 'Subscription Date'
    df = df.withColumn("month", month(col("Subscription Date"))) \
           .withColumn("year", year(col("Subscription Date")))
    
    #create new column with month and year combined
    df = df.withColumn("month_year", concat_ws("-", format_string("%02d", month(col("Subscription Date"))), year(col("Subscription Date"))))
    
    #extract http, https from website using split func
    df = df.withColumn("Web_protocol", split(col("Website"), "://").getItem(0))
    
    #df.select("First Name", "Last Name", "fullname", "fullname_upper", "Subscription Date", "month_year").show()
    
    # Define your column rename mapping
    rename_dict = {
        "month_year": "Month - Year",
        "Web_protocol": "Web Protocol",
        "fullname_upper": "Full name"
        # Add more pairs as needed: "old_name": "new_name"
    }
    
    # Apply renaming
    for old_name, new_name in rename_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    #group data based on month and year column
    df_grouped = df.groupBy("month", "year", "Month - Year").agg(count("*").alias("total_records"))
    
    df_grouped = df_grouped.orderBy("year", "month")
    
    df_grouped = df_grouped.select("Month - Year", "total_records")
    
    #df_grouped.show()
    
    #print selected columns
    df.select("First Name", "Last Name", "Full name", "Subscription Date", "Month - Year", "Website", "Web Protocol").show()
    
    df = df.select("First Name", "Last Name", "Full name", "Subscription Date", "Month - Year", "Website", "Web Protocol")
    
    #writetocsv(df)
    
    dfren = df
    
    rename_dict = {
        "First Name": "firstname",
        "Last Name": "lastname",
        "Full name": "fullname",
        "Subscription Date": "subscriptiondate",
        "Month - Year": "month_year",
        "Website": "website",
        "Web Protocol": "webprotocol"
    }
    
    # Apply renaming
    for old_name, new_name in rename_dict.items():
        dfren = dfren.withColumnRenamed(old_name, new_name)
    
    dfren.show()
    
    #writetos3(dfren)
    writetoredshiftdb(dfren)

def writetoredshiftdb(spark_df):
    # Write to Redshift
    USERNAME               = 'abc-user'  # USERNAME FOR CLUSTER
    PASSWORD               = 'password'  # PASSWORD
    REDSHIFT_DATABASE_NAME = 'dbname'
    REDSHIFT_ROLE          = 'arn:aws:iam::123456789:role/redshift_role'
    #HOST                   = "hostname"
    REDSHIFT_DRIVER        = "redshift-driver"
    REDSHIFT_JDBC_URL      = f"jdbc_url-withport-dbname"
    REDSHIFT_URL           = REDSHIFT_JDBC_URL + "?user=" + USERNAME + "&password=" + PASSWORD
    IAM_ROLE_ARN           = REDSHIFT_ROLE

    #glueContext.write_dynamic_frame.from_jdbc_conf(
    #frame=dynamic_frame,
    #catalog_connection="your-redshift-connection-name",  # 👈 Glue connection to Redshift
    #connection_options={
    #    "dbtable": "test.table",
    #    "database": "databasename"
    #},
    redshift_tmp_dir="s3://vertica-data/redshift-temp-folder/"  # 👈 temp S3 path for staging
    
    spark_df.coalesce(1).write \
        .format(REDSHIFT_DRIVER) \
        .option("url", REDSHIFT_URL) \
        .option("dbtable", f"test.customers1") \
        .option("tempdir", redshift_tmp_dir) \
        .option("tempformat", "csv") \
        .option("aws_iam_role", IAM_ROLE_ARN) \
        .mode("overwrite") \
        .save()

    print("✅ Data successfully written to Redshift.")

if __name__ == '__main__':
    main()

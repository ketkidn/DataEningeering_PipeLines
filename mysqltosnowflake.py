import pandas as pd
import mysql.connector
import boto3
import snowflake.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="<<Password>>",
    database="adf"
)

# Load data into pandas
query = "SELECT emp_id,emp_name,emp_dob,emp_email,emp_Adddate FROM emp_details"
df = pd.read_sql(query, conn)

# ========== STEP 3: Save as Parquet ==========
parquet_file = "emp_details.parquet"
df.to_parquet(parquet_file, index=False, engine="pyarrow")
print(f"✅ Parquet file created: {parquet_file}")

# ========== STEP 4: Upload to S3 ==========
s3 = boto3.client(
    "s3",
    aws_access_key_id="<<ID>>",
    aws_secret_access_key="<<Key>>",
    region_name="eu-north-1"  # e.g., Mumbai region (change as needed)
)

bucket_name = "<<bucketname>>"
s3_key = "emp_details.parquet"  # folder/key in S3

s3.upload_file(parquet_file, bucket_name, s3_key)

print(f"✅ File uploaded to s3://{bucket_name}/{s3_key}")

# Connect to Snowflake
conn = snowflake.connector.connect(
    user="<<username>>",
    password="<<password>>",
    account="<<accountname>>",  # e.g. xy12345.ap-south-1
    warehouse="<<warehousename>>",
    database="<<dbname>>",
    schema="PUBLIC"
)
cur = conn.cursor()

# 1. Create a file format (if not exists)
cur.execute("""
CREATE OR REPLACE FILE FORMAT my_parquet_format
TYPE = PARQUET;
""")

# 2. Create a stage pointing to your S3 bucket
cur.execute("""
CREATE OR REPLACE STAGE my_s3_stage
URL='<<URL>>'
CREDENTIALS=(AWS_KEY_ID='<<ID>>' AWS_SECRET_KEY='<<Key>>')
FILE_FORMAT = my_parquet_format;
""")

# 3. Create target table if not exists
cur.execute("""
create table emp_Details
(
emp_id int,
emp_name varchar(20),
emp_dob datetime,
emp_email varchar(50),
emp_adddate datetime
);
""")

# 4. Copy data from S3 to Snowflake
cur.execute("""
COPY INTO emp_details
FROM @my_s3_stage/emp_details.parquet
FILE_FORMAT = (FORMAT_NAME = my_parquet_format)
ON_ERROR = 'CONTINUE';
""")

print("✅ Data copied successfully from S3 → Snowflake!")

cur.close()
conn.close()
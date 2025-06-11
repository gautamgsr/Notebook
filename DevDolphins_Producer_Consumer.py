# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from io import StringIO
from datetime import datetime as t
import time,boto3,os,psycopg2,pandas as pd



access_key_id  = "xxxxxxxxxxxxxxx" 
access_key_pwd = "xxxxxxxxxxxxxxx"
postgres_pwd   = 'xxxxxxxxxxxxxxx' 


os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = access_key_pwd

boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=access_key_pwd)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", access_key_pwd)
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


jar_path = "dbfs:/FileStore/postgresql_42_7_6.jar"
sc._jsc.addJar(jar_path)

jdbc_url = "jdbc:postgresql://database-2.czy4sq8iitbm.ap-south-1.rds.amazonaws.com:5432/postgres"
properties = {
    "user": "postgres",
    "password": postgres_pwd,
    "driver": "org.postgresql.Driver"
}


# COMMAND ----------

def update_postgres_data(df):

    conn,cur = postgres_cur()

    grouped_df = df.groupBy(col('customer'), col('merchant')).agg(F.count("*").alias("txn_count"),F.avg("amount").alias("avg_amount"))
    grouped_df.write.jdbc(url=jdbc_url,table="staging_customer_merchant_txn_counts", mode="overwrite",properties=properties)
    upsert_sql = """
                    INSERT INTO customer_merchant_txn_counts (customer, merchant, txn_count, avg_amount)
                    SELECT customer, merchant, txn_count, avg_amount
                    FROM staging_customer_merchant_txn_counts
                    ON CONFLICT (customer, merchant)
                    DO UPDATE 
                    SET txn_count = customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count,
                        avg_amount = (
                        (customer_merchant_txn_counts.avg_amount * customer_merchant_txn_counts.txn_count + EXCLUDED.avg_amount * EXCLUDED.txn_count) /
                        (customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count)
                        );
                """
    cur.execute(upsert_sql)
    conn.commit()

    cur.execute("DROP TABLE IF EXISTS staging_customer_merchant_txn_counts")
    conn.commit()

    grouped_gender = df.groupBy("merchant").agg(F.sum(F.when(col("gender") == "'M'", 1).otherwise(0)).alias("male"),F.sum(F.when(col("gender") == "'F'", 1).otherwise(0)).alias("female"))
    grouped_gender.write.jdbc(url=jdbc_url,table="staging_merchant_gender_counts", mode="overwrite",properties=properties)

    upsert_sql1 ="""
                    INSERT INTO merchant_gender_counts (merchant,male,female)
                    SELECT merchant, male, female
                    FROM staging_merchant_gender_counts
                    ON CONFLICT (merchant)
                    DO UPDATE 
                    SET male   = merchant_gender_counts.male + EXCLUDED.male,
                        female = merchant_gender_counts.female + EXCLUDED.female;
                """
        
    cur.execute(upsert_sql1)
    conn.commit()
    
    cur.execute("DROP TABLE IF EXISTS staging_merchant_gender_counts")
    conn.commit()

    cur.close()
    conn.close()


def pat1(jdbc_url,properties):


    customer_merchant_txn_counts = spark.read.jdbc(url=jdbc_url, table="customer_merchant_txn_counts", properties=properties)

    window_spec_by_merchant = Window.partitionBy("merchant").orderBy(F.desc("txn_count"))

    ranked_df = customer_merchant_txn_counts.withColumn("txn_rank", F.rank().over(window_spec_by_merchant))
    ranked_df = ranked_df.withColumn("total_txn", F.sum("txn_count").over(window_spec_by_merchant))

    top_1pct_customers_df = ranked_df.filter((col("txn_rank") <= F.greatest(F.lit(1), (col("total_txn") * 0.01).cast("int"))))
    top_1pct_customers_df = top_1pct_customers_df.select("customer", "merchant", "txn_count", "txn_rank", "total_txn")

    bottom_1pct_weights_df = spark.read.jdbc(url=jdbc_url, table="bottom_1pct_weights", properties=properties)
    joined_df = top_1pct_customers_df.alias("t").join(bottom_1pct_weights_df.alias("b"),on=["customer", "merchant"],how="inner")

    # Add the additional columns
    pattern_matches_df = joined_df.withColumn("ystarttime", F.current_timestamp())
    pattern_matches_df = pattern_matches_df.withColumn("detectiontime", F.current_timestamp())
    pattern_matches_df = pattern_matches_df.withColumn("patternid", F.lit("PatId1"))
    pattern_matches_df = pattern_matches_df.withColumn("actiontype",F.lit("UPGRADE"))


    # Select required columns in order
    pattern_matches_df = pattern_matches_df.select(
        "ystarttime",
        "detectiontime",
        "patternid",
        "actiontype",
        "customer",
        "merchant"
    )

    return pattern_matches_df

def pat2(jdbc_url,properties):

    customer_merchant_txn_counts = spark.read.jdbc(url=jdbc_url, table="customer_merchant_txn_counts", properties=properties) 
    
    child_customers_df = customer_merchant_txn_counts.filter((col("avg_amount") < 23) & (col("txn_count") >= 80))
    child_customers_df = child_customers_df.withColumn("ystarttime", F.current_timestamp())
    child_customers_df = child_customers_df.withColumn("detectiontime", F.current_timestamp())
    child_customers_df = child_customers_df.withColumn("patternid", F.lit("PatId2"))
    child_customers_df = child_customers_df.withColumn("actiontype",F.lit("CHILD"))
    child_customers_df = child_customers_df.select(
        "ystarttime",
        "detectiontime",
        "patternid",
        "actiontype",
        "customer",
        "merchant"
    )
    return child_customers_df

def pat3(jdbc_url,properties):

    merchant_gender = spark.read.jdbc(url=jdbc_url, table="merchant_gender_counts", properties=properties)
    merchant_gender = merchant_gender.filter((col('male') > col('female')) & (col('female') > 100))

    merchant_gender = merchant_gender.withColumn("ystarttime", F.current_timestamp())
    merchant_gender = merchant_gender.withColumn("detectiontime", F.current_timestamp())
    merchant_gender = merchant_gender.withColumn("patternid", F.lit("PatId3"))
    merchant_gender = merchant_gender.withColumn("actiontype",F.lit("DEI-NEEDED"))

    merchant_gender = merchant_gender.select(
        "ystarttime",
        "detectiontime",
        "patternid",
        "actiontype",
        F.lit("").alias("customer"),
        "merchant"
    )
    return merchant_gender

def postgres_cur():
    host = "database-2.czy4sq8iitbm.ap-south-1.rds.amazonaws.com"
    port = "5432"
    db   = "postgres"
    user_name = "postgres"
    user_pass = postgres_pwd
    conn = psycopg2.connect(host=host,port=port,database=db,user=user_name,password=user_pass)
    return conn,conn.cursor()

def get_next_chunk():
    conn,cur = postgres_cur()
    cur.execute("""
                            SELECT id, chunk_index, s3_path 
                            FROM chunk_tracking 
                            WHERE status = 'written' 
                            ORDER BY chunk_index ASC 
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED;
                    """)
    data = cur.fetchone()
    cur.close()
    conn.close()
    return data

def dedupe_final_output():
    
    conn,cur = postgres_cur()
    cur.execute(
                """
                    WITH ranked_rows AS (
                        SELECT ctid,
                            ROW_NUMBER() OVER (
                                PARTITION BY customer, merchant
                                ORDER BY ystarttime ASC
                            ) AS rn
                        FROM final_output
                    )
                    DELETE FROM final_output
                    WHERE ctid IN (
                        SELECT ctid FROM ranked_rows WHERE rn > 1);
                """)
    conn.commit() 
    # cur.close()
    # conn.close()
    return True


def update_chunk_status(chunk_index, status):
    conn,cur = postgres_cur()

    cur.execute("BEGIN;")

    # Lock the row first
    cur.execute("""
                    SELECT * FROM chunk_tracking
                    WHERE chunk_index = %s
                    FOR UPDATE;
                """, (chunk_index,))

    # Now do the update safely
    cur.execute("""
                    UPDATE chunk_tracking 
                    SET status = %s, updated_at = %s 
                    WHERE chunk_index = %s;
                """, (status, datetime.utcnow(), chunk_index))

    cur.execute("COMMIT;")

    conn.commit()
    cur.close()
    conn.close()

def list_s3_files(bucket_name, prefix):

    s3_client = boto3.client('s3')
    file_list = []
    
    
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_list.append(obj['Key'])

        file_list = [file for file in file_list if file.lower().endswith('.csv')]
        return file_list[0]
    
    except Exception as e:
        print(f"Error: {e}")
        return []
    
def generate_output():

    conn,cur = postgres_cur()
    query = """         
                    WITH to_publish AS (
                        SELECT merchant, customer
                        FROM final_output
                        WHERE published IS NULL
                        ORDER BY ystarttime
                        LIMIT 50
                        FOR UPDATE SKIP LOCKED),
                check_count AS (
                    SELECT COUNT(*) AS cnt FROM to_publish
                ),
                update_rows AS (
                    UPDATE final_output
                    SET published = 'published'
                    WHERE (merchant, customer) IN (
                        SELECT merchant, customer FROM to_publish
                    )
                    AND EXISTS (
                        SELECT 1 FROM check_count WHERE cnt = 50
                    )
                    RETURNING *
                )
                SELECT * FROM update_rows;
            """
    cur.execute("BEGIN;")
    cur.execute(query)
    published_rows = cur.fetchall()
    cur.execute("COMMIT;")

    schema = StructType([
    StructField("ystarttime", TimestampType(), True),
    StructField("detectiontime",TimestampType(), True),
    StructField("patternid",StringType(), True),
    StructField("actiontype",StringType(), True),
    StructField("customer",StringType(), True),
    StructField("merchant",StringType(), True),
    StructField("published",StringType(), True)
    ])

    # column_names = [desc[0] for desc in cur.description]
    df = spark.createDataFrame(published_rows,schema=schema)
    df = df.select(col("ystarttime"),col("detectiontime"),col("patternid"),col("actiontype"),col("customer"),col("merchant"))
    cur.close()
    conn.close()

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions and theirs descriptions

# COMMAND ----------

# Function to update aggregated transaction data into PostgreSQL
def update_postgres_data(df):
    conn, cur = postgres_cur()

    # Group by customer and merchant, aggregate transaction count and average amount
    grouped_df = df.groupBy(col('customer'), col('merchant')).agg(
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount")
    )

    # Write to a staging table in PostgreSQL
    grouped_df.write.jdbc(
        url=jdbc_url,
        table="staging_customer_merchant_txn_counts",
        mode="overwrite",
        properties=properties
    )

    # Upsert the data into the main table
    upsert_sql = """
        INSERT INTO customer_merchant_txn_counts (customer, merchant, txn_count, avg_amount)
        SELECT customer, merchant, txn_count, avg_amount
        FROM staging_customer_merchant_txn_counts
        ON CONFLICT (customer, merchant)
        DO UPDATE 
        SET txn_count = customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count,
            avg_amount = (
                (customer_merchant_txn_counts.avg_amount * customer_merchant_txn_counts.txn_count + 
                 EXCLUDED.avg_amount * EXCLUDED.txn_count) /
                (customer_merchant_txn_counts.txn_count + EXCLUDED.txn_count)
            );
    """
    cur.execute(upsert_sql)
    conn.commit()

    # Drop the staging table
    cur.execute("DROP TABLE IF EXISTS staging_customer_merchant_txn_counts")
    conn.commit()

    # Group by merchant, count gender distribution
    grouped_gender = df.groupBy("merchant").agg(
        F.sum(F.when(col("gender") == "'M'", 1).otherwise(0)).alias("male"),
        F.sum(F.when(col("gender") == "'F'", 1).otherwise(0)).alias("female")
    )

    # Write gender counts to staging table
    grouped_gender.write.jdbc(
        url=jdbc_url,
        table="staging_merchant_gender_counts",
        mode="overwrite",
        properties=properties
    )

    # Upsert gender counts into the main table
    upsert_sql1 = """
        INSERT INTO merchant_gender_counts (merchant, male, female)
        SELECT merchant, male, female
        FROM staging_merchant_gender_counts
        ON CONFLICT (merchant)
        DO UPDATE 
        SET male = merchant_gender_counts.male + EXCLUDED.male,
            female = merchant_gender_counts.female + EXCLUDED.female;
    """
    cur.execute(upsert_sql1)
    conn.commit()

    # Drop the staging gender table
    cur.execute("DROP TABLE IF EXISTS staging_merchant_gender_counts")
    conn.commit()

    cur.close()
    conn.close()

# Pattern 1: Top 1% high-transacting customers per merchant
def pat1(jdbc_url, properties):
    customer_merchant_txn_counts = spark.read.jdbc(
        url=jdbc_url,
        table="customer_merchant_txn_counts",
        properties=properties
    )

    # Define a window to rank by transaction count
    window_spec_by_merchant = Window.partitionBy("merchant").orderBy(F.desc("txn_count"))

    ranked_df = customer_merchant_txn_counts.withColumn("txn_rank", F.rank().over(window_spec_by_merchant))
    ranked_df = ranked_df.withColumn("total_txn", F.sum("txn_count").over(window_spec_by_merchant))

    # Filter top 1% customers
    top_1pct_customers_df = ranked_df.filter(
        (col("txn_rank") <= F.greatest(F.lit(1), (col("total_txn") * 0.01).cast("int")))
    ).select("customer", "merchant", "txn_count", "txn_rank", "total_txn")

    bottom_1pct_weights_df = spark.read.jdbc(
        url=jdbc_url,
        table="bottom_1pct_weights",
        properties=properties
    )

    # Join with weight data
    joined_df = top_1pct_customers_df.alias("t").join(
        bottom_1pct_weights_df.alias("b"),
        on=["customer", "merchant"],
        how="inner"
    )

    # Add metadata columns
    pattern_matches_df = joined_df.withColumn("ystarttime", F.current_timestamp())
    pattern_matches_df = pattern_matches_df.withColumn("detectiontime", F.current_timestamp())
    pattern_matches_df = pattern_matches_df.withColumn("patternid", F.lit("PatId1"))
    pattern_matches_df = pattern_matches_df.withColumn("actiontype", F.lit("UPGRADE"))

    # Select final output columns
    pattern_matches_df = pattern_matches_df.select(
        "ystarttime", "detectiontime", "patternid", "actiontype", "customer", "merchant"
    )

    return pattern_matches_df

# Pattern 2: Child customers based on avg amount and txn count
def pat2(jdbc_url, properties):
    customer_merchant_txn_counts = spark.read.jdbc(
        url=jdbc_url,
        table="customer_merchant_txn_counts",
        properties=properties
    )

    # Identify customers with low avg amount but high txn count
    child_customers_df = customer_merchant_txn_counts.filter(
        (col("avg_amount") < 23) & (col("txn_count") >= 80)
    )

    # Add metadata
    child_customers_df = child_customers_df.withColumn("ystarttime", F.current_timestamp())
    child_customers_df = child_customers_df.withColumn("detectiontime", F.current_timestamp())
    child_customers_df = child_customers_df.withColumn("patternid", F.lit("PatId2"))
    child_customers_df = child_customers_df.withColumn("actiontype", F.lit("CHILD"))

    # Select final output columns
    child_customers_df = child_customers_df.select(
        "ystarttime", "detectiontime", "patternid", "actiontype", "customer", "merchant"
    )

    return child_customers_df

# Pattern 3: Gender imbalance at merchant level
def pat3(jdbc_url, properties):
    merchant_gender = spark.read.jdbc(
        url=jdbc_url,
        table="merchant_gender_counts",
        properties=properties
    )

    # Identify merchants with gender imbalance
    merchant_gender = merchant_gender.filter((col('male') > col('female')) & (col('female') > 100))

    # Add metadata
    merchant_gender = merchant_gender.withColumn("ystarttime", F.current_timestamp())
    merchant_gender = merchant_gender.withColumn("detectiontime", F.current_timestamp())
    merchant_gender = merchant_gender.withColumn("patternid", F.lit("PatId3"))
    merchant_gender = merchant_gender.withColumn("actiontype", F.lit("DEI-NEEDED"))

    # Final selection with customer as empty
    merchant_gender = merchant_gender.select(
        "ystarttime", "detectiontime", "patternid", "actiontype",
        F.lit("").alias("customer"),
        "merchant"
    )

    return merchant_gender

# PostgreSQL connection helper
def postgres_cur():
    host = "database-2.czy4sq8iitbm.ap-south-1.rds.amazonaws.com"
    port = "5432"
    db = "postgres"
    user_name = "postgres"
    user_pass = postgres_pwd
    conn = psycopg2.connect(host=host, port=port, database=db, user=user_name, password=user_pass)
    return conn, conn.cursor()

# Get next data chunk to process
def get_next_chunk():
    conn, cur = postgres_cur()
    cur.execute("""
        SELECT id, chunk_index, s3_path 
        FROM chunk_tracking 
        WHERE status = 'written' 
        ORDER BY chunk_index ASC 
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    """)
    data = cur.fetchone()
    cur.close()
    conn.close()
    return data

# De-duplicate entries in final_output based on customer + merchant + ystarttime
def dedupe_final_output():
    conn, cur = postgres_cur()
    cur.execute("""
        WITH ranked_rows AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer, merchant
                       ORDER BY ystarttime ASC
                   ) AS rn
            FROM final_output
        )
        DELETE FROM final_output
        WHERE ctid IN (
            SELECT ctid FROM ranked_rows WHERE rn > 1
        );
    """)
    conn.commit()
    # cur.close()
    # conn.close()
    return True

# Update the status of a chunk after processing
def update_chunk_status(chunk_index, status):
    conn, cur = postgres_cur()

    cur.execute("BEGIN;")

    # Lock the row first to prevent race conditions
    cur.execute("""
        SELECT * FROM chunk_tracking
        WHERE chunk_index = %s
        FOR UPDATE;
    """, (chunk_index,))

    # Update the status with timestamp
    cur.execute("""
        UPDATE chunk_tracking 
        SET status = %s, updated_at = %s 
        WHERE chunk_index = %s;
    """, (status, t.utcnow(), chunk_index))

    cur.execute("COMMIT;")
    conn.commit()

    cur.close()
    conn.close()

# List CSV files in S3 path
def list_s3_files(bucket_name, prefix):
    s3_client = boto3.client('s3')
    file_list = []

    if prefix and not prefix.endswith('/'):
        prefix += '/'

    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_list.append(obj['Key'])

        # Filter for .csv files
        file_list = [file for file in file_list if file.lower().endswith('.csv')]
        return file_list[0]

    except Exception as e:
        print(f"Error: {e}")
        return []

# Generate the final output to be published
def generate_output():
    conn, cur = postgres_cur()
    query = """         
        WITH to_publish AS (
            SELECT merchant, customer
            FROM final_output
            WHERE published IS NULL
            ORDER BY ystarttime
            LIMIT 50
            FOR UPDATE SKIP LOCKED
        ),
        check_count AS (
            SELECT COUNT(*) AS cnt FROM to_publish
        ),
        update_rows AS (
            UPDATE final_output
            SET published = 'published'
            WHERE (merchant, customer) IN (
                SELECT merchant, customer FROM to_publish
            )
            AND EXISTS (
                SELECT 1 FROM check_count WHERE cnt = 50
            )
            RETURNING *
        )
        SELECT * FROM update_rows;
    """
    cur.execute("BEGIN;")
    cur.execute(query)
    published_rows = cur.fetchall()
    cur.execute("COMMIT;")

    schema = StructType([
        StructField("ystarttime", TimestampType(), True),
        StructField("detectiontime", TimestampType(), True),
        StructField("patternid", StringType(), True),
        StructField("actiontype", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("published", StringType(), True)
    ])

    # Convert to Spark DataFrame
    df = spark.createDataFrame(published_rows, schema=schema)
    df = df.select(
        col("ystarttime"),
        col("detectiontime"),
        col("patternid"),
        col("actiontype"),
        col("customer"),
        col("merchant")
    )

    cur.close()
    conn.close()
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Producer Process which creates chunks of 10K records every nth second to S3

# COMMAND ----------

# Parameters
input_csv_path = "s3://devdolphins007/Source/transactions.csv"   # Change to your CSV file path
output_s3_prefix = "s3://devdolphins007/raw/chunk_"  # Output path prefix
chunk_size = 10000
sleep_seconds = 3

conn,cur = postgres_cur()

# Load CSV
df = spark.read.csv(input_csv_path, header=True, inferSchema=True)

# Total row count
total_rows = df.count()
total_chunks = (total_rows + chunk_size - 1) // chunk_size

# Loop through chunks
for i in range(total_chunks):
    offset = i * chunk_size
    chunk_df = df.limit(chunk_size + offset).subtract(df.limit(offset))

    output_path = f"{output_s3_prefix}{i}"
    record_count = chunk_df.count()

    # Write to S3
    chunk_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    
    # Insert into PostgreSQL
    try:
        cur.execute("""
                            INSERT INTO chunk_tracking (chunk_index, s3_path, record_count, status, created_at)
                            VALUES (%s, %s, %s, %s, %s)
                    """,(i, output_path, record_count, 'written',t.utcnow()))
        
        conn.commit()
        print(f"[{i+1}/{total_chunks}] Written to S3 and logged to DB: {output_path}")

    except Exception as e:
        print(f"DB insert failed for chunk {i}: {e}")
        conn.rollback()
    time.sleep(sleep_seconds)
cur.close()
conn.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumer process which consumer 10k records from S3 in seqeunce 

# COMMAND ----------

# PostgreSQL setup
pg_conn,pg_cursor = postgres_cur()
# S3 setup
s3 = boto3.client('s3')  # assumes credentials are set via env, IAM role, or config

try:
    while True:
        
        chunk = get_next_chunk()
        if not chunk:
            print("No more chunks to process.")
            break
        
        chunk_id, chunk_index, s3_path = chunk
        print(f"Picked chunk {chunk_index} from {s3_path}")

        try:

            bucket = "devdolphins007"
            key   =  f"raw/chunk_{chunk_index}"

            file_name = list_s3_files(bucket,key)
            key = file_name


            try:
                s3.head_object(Bucket=bucket, Key=key)
            except Exception as e:
                print(f"S3 head object is failing {chunk_index}:{e}")                      
            
            try:
                # print(f"s3://{bucket}/{file_name}")
                df = spark.read.option("header", "true").csv(f"s3://{bucket}/{file_name}")
            except Exception as e:
                print(f'Can\'t load DF {chunk_index}:{e}')

            try:
                update_postgres_data(df)
                pat1_df = pat1(jdbc_url,properties)
                pat2_df = pat2(jdbc_url,properties)
                pat3_df = pat3(jdbc_url,properties)

                union_df = pat1_df.unionByName(pat2_df).unionByName(pat3_df)
                # .withColumn('published',F.lit(None))
            
                union_df.write.jdbc(url=jdbc_url,table="final_output", mode="append",properties=properties)

                if dedupe_final_output():
                    output_df = generate_output()
                    if output_df.count() ==50:
                        output_s3_prefix = "s3://devdolphins007/processed/chunk_"
                        output_path = f"{output_s3_prefix}{chunk_index}"
                        output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
                        print(f'output written {chunk_index}')

                update_chunk_status(chunk_index, 'processed')
                print(f"Chunk {chunk_index} processed.")
            
            except Exception as e:
                print(f'Processing failed {chunk_index}:{e}')
                update_chunk_status(chunk_index, 'failed')

            # time.sleep(1)

        except Exception as e:
            print(f"Error processing chunk {chunk_index}: {e}")
            update_chunk_status(chunk_index, 'failed')

finally:
    pg_cursor.close()
    pg_conn.close()
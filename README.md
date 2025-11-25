# Real-Time-PySpark-Streaming-Data-Pipeline-with-AutoLoader

## Overview

This notebook demonstrates how to set up a data pipeline using PySpark's Auto Loader feature to continuously ingest CSV files into a Delta table in Databricks. The pipeline will automatically infer the schema or use a predefined schema, process the incoming data, and write it to the `bronze_table` in the `rashid` database.

## Table of Contents

1. **Schema Definition**
2. **Data Ingestion Setup**
3. **Auto Loader Configuration**
4. **Streaming Write to Delta Table**
5. **Clean-Up Operations**

## 1. Schema Definition

We define the schema for the incoming data using the `StructType` and `StructField` classes from `pyspark.sql.types`. The schema includes fields such as:

- `VendorID`: Integer
- `tpep_pickup_datetime`: Timestamp
- `tpep_dropoff_datetime`: Timestamp
- `passenger_count`: Integer
- `trip_distance`: Double
- ... (additional fields as specified)

```python
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, DoubleType, TimestampType, StringType

schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),  
    StructField("DOLocationID", IntegerType()),  
    StructField("payment_type", IntegerType()),  
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("Airport_fee", DoubleType())
])
```

## 2. Data Ingestion Setup

We begin by cleaning up the existing data by truncating the `bronze_table` and removing any old checkpoint and schema location directories to ensure a fresh start.

```sql
%sql
truncate table rashid.bronze_table
```

```python
%fs rm -r /FileStore/rashid/autoloader/sl
%fs rm -r /FileStore/rashid/autoloader/cp
%fs mkdirs /FileStore/rashid/autoloader/cp
%fs mkdirs /FileStore/rashid/autoloader/sl
```

## 3. Auto Loader Configuration

We configure the Auto Loader to read CSV files from a specified directory in DBFS. The following options are set:

- **Format**: `csv`
- **Schema Location**: Directory to store inferred schema.
- **Header**: Set to `true` to read the header from the CSV files.
- **Schema**: Predefined schema to enforce data structure.

```python
dbfs_path = "dbfs:/FileStore/rashid/poc_folder"
sl= "dbfs:/FileStore/rashid/autoloader/sl/"

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv") 
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaLocation", sl)   
      .option('header', 'true') 
      .schema(schema)  
      .load(dbfs_path))
```

## 4. Streaming Write to Delta Table

The incoming data is then written to the Delta table `bronze_table` with the following configurations:

- **Merge Schema**: Set to `true` to accommodate schema evolution.
- **Checkpoint Location**: Directory for storing checkpoint information.
- **Output Mode**: Set to `append` to add new records.
- **Trigger**: Configured to run once for this operation.

```python
cp= "dbfs:/FileStore/rashid/autoloader/cp/"
query = (df.writeStream.format("delta")
    .option("mergeSchema", "true")
    .option("checkpointLocation", cp)
    .outputMode("append")
    .trigger(once=True)
    .table("rashid.bronze_table"))
```

## 5. Clean-Up Operations

After the stream processing, it’s advisable to clean up any temporary files and directories if necessary. Ensure that you manage your storage effectively by monitoring the size of your checkpoint and schema locations.


# README for Data Processing Pipeline: From Bronze to Silver Table

## Overview

This notebook outlines the steps to process streaming data from a Delta table (`bronze_table`) into a refined Delta table (`silver_table`) using PySpark. The transformation includes handling missing values, duplicates, incorrect data types, and computing additional metrics.

## Table of Contents

1. **Schema Cleanup and Setup**
2. **Data Ingestion**
3. **Data Transformation**
4. **Statistical Analysis**
5. **Data Validation**
6. **Final Write to Silver Table**

## 1. Schema Cleanup and Setup

We begin by truncating the `silver_table` and cleaning up any previous checkpoint data to ensure a fresh processing environment.

```sql
%sql
truncate TABLE rashid.silver_table;
```

```python
%fs rm -r FileStore/rashid/autoloader/sil_cp
%fs mkdirs FileStore/rashid/autoloader/sil_cp
```

## 2. Data Ingestion

We read streaming data from the `bronze_table` into a DataFrame. The schema is inferred automatically.

```python
silver_df = spark.readStream.format("delta").option("inferSchema", "true").table("rashid.bronze_table")
```

## 3. Data Transformation

In this section, we apply various transformations:

- **Handle Missing Values**: Set `fare_amount` to `None` if it exceeds a certain threshold.
- **Introduce Duplicates**: For testing, we add 10 duplicate rows.
- **Incorrect Data Types**: Change `total_amount` to string type for demonstration.

```python
from pyspark.sql.functions import when, col, lit 

df_with_missing = silver_df.withColumn("fare_amount", when(col("fare_amount") > 45, lit(None)).otherwise(col("fare_amount")))
df_with_duplicates = df_with_missing.union(df_with_missing.limit(10))  # Adding 10 duplicate rows
df_with_incorrect_types = df_with_duplicates.withColumn("total_amount", col("total_amount").cast("string"))
```

## 4. Statistical Analysis

We calculate the count of missing values across all columns and compute basic statistics for numeric columns (`trip_distance`, `fare_amount`, and `total_amount`).

```python
from pyspark.sql.functions import count, isnan, mean, stddev

numeric_columns = [c for c, dtype in silver_df.dtypes if dtype in ('double', 'float', 'int')]

# Check for missing values
missing_values_count = silver_df.select([
    count(when(isnan(c) | col(c).isNull(), c)).alias(c) if c in numeric_columns 
    else count(when(col(c).isNull(), c)).alias(c) for c in silver_df.columns
])

# Display missing values count
# display(missing_values_count)

# Compute mean and standard deviation
stats = silver_df.select([mean(col(c)).alias(f"mean_{c}") for c in numeric_columns] +
                         [stddev(col(c)).alias(f"stddev_{c}") for c in numeric_columns])
# display(stats)
```

## 5. Data Validation

We validate the data by filtering out unrealistic values and checking for type mismatches based on an expected schema.

```python
# Filter out unrealistic values
silver_df = silver_df.filter((col("trip_distance") < 100) & (col("fare_amount") < 500))

# Define the expected schema
expected_schema = {
    "VendorID": IntegerType(), 
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count": IntegerType(),
    "trip_distance": DoubleType(),
    "RatecodeID": LongType(),
    "store_and_fwd_flag": StringType(),
    "PULocationID": IntegerType(),  
    "DOLocationID": IntegerType(),  
    "payment_type": IntegerType(),  
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "Airport_fee": DoubleType()
}

# Check for mismatched data types
for col_name, expected_type in expected_schema.items():
    actual_type = [f.dataType for f in silver_df.schema.fields if f.name == col_name][0]
    if type(actual_type) != type(expected_type):
        print(f"Column '{col_name}' has incorrect data type. Expected: {expected_type.simpleString()}, Actual: {actual_type.simpleString()}")
```

## 6. Final Write to Silver Table

After corrections and validations, we write the refined DataFrame to the `silver_table`. This includes additional transformations to compute time duration and tax percentage.

```python
from pyspark.sql import functions as F

silver_df = df_corrected.withColumn(
    'time_duration_seconds',
    F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')
).withColumn(
    'time_format',
    F.concat(
        F.lpad(F.floor(F.col('time_duration_seconds') / 3600), 2, '0'), F.lit(':'),
        F.lpad(F.floor((F.col('time_duration_seconds') % 3600) / 60), 2, '0'), F.lit(':'),
        F.lpad(F.col('time_duration_seconds') % 60, 2, '0')
    )
).withColumn(
    'tax_percentage',
    F.round(((F.col('total_amount') - F.col('fare_amount')) / F.col('total_amount')) * 100, 2)
)

# Write the streaming DataFrame to a Delta table
sil_cp = "FileStore/rashid/autoloader/sil_cp"

query = (silver_df.writeStream
         .format("delta")
         .outputMode("append")  # Output mode is set to 'append'
         .option("mergeSchema", "true")
         .option("checkpointLocation", sil_cp)  # Checkpointing
         .trigger(availableNow=True) 
         .table("rashid.silver_table"))  # Write to the silver table
```

## Conclusion

This notebook provides a comprehensive framework for processing streaming data from a bronze to a silver Delta table. Each transformation step is crucial for ensuring data quality and preparing the dataset for further analysis. Adjust the code and schema as needed based on specific data requirements.

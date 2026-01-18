"""This  file will be used for transformation and cleaning of the datasets"""

# set up the uri to pull from my storage account
my_scope = "storageScope"
my_key="storage-access-key"
storage_end_point = "csci422storageblob.dfs.core.windows.net"
container_name = "project"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key)
)

# Set URI for saving data
uri = "abfss://" + container_name + "@" + storage_end_point + "/"
print(uri)

# imports go here
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    regexp_replace, split, col, trim, lower, lit, monotonically_increasing_id
)
from pyspark.sql.types import IntegerType, DoubleType

# Read Bronze Tables
used_cars_df = spark.read.format("delta").load(uri + "bronze/used_cars")
new_car_df  = spark.read.format("delta").load(uri + "bronze/new_car_msrp")

# Normalize Column Names
# Used Cars
used_cars_df = used_cars_df.toDF(*[c.lower().replace(" ", "_") for c in used_cars_df.columns])
used_cars_df = used_cars_df.withColumn("brand", lower(trim(col("brand"))))
used_cars_df = used_cars_df.withColumn("model", lower(trim(col("model"))))

# New Cars
new_car_df = new_car_df.toDF(*[c.lower().replace(" ", "_") for c in new_car_df.columns])
new_car_df = new_car_df.withColumn("companyname", lower(trim(col("companyname"))))
new_car_df = new_car_df.withColumn("carname", lower(trim(col("carname"))))

# Standardize Price Columns
# Cast used car price to double
used_cars_df = used_cars_df.withColumn("price", col("price").cast("double"))

# Clean and cast new car price
new_car_df = (
    new_car_df
    .withColumn("carprice", regexp_replace(col("carprice"), "[$, ]", ""))
    .withColumn("carprice", split(col("carprice"), "-").getItem(0))
    .withColumn("carprice", col("carprice").cast("double"))
)

# Clean New Cars Columns
new_car_df = (
    new_car_df
    .withColumn("horsepower", regexp_replace(col("horsepower"), " hp", "").cast("int"))
    .withColumn("totalspeed", regexp_replace(col("totalspeed"), "(?i) km/h", "").cast("int"))
    .withColumn("fueltype", lower(regexp_replace(col("fueltype"), "(?i)hyrbrid|hyrbid", "hybrid")))
    .dropDuplicates()
)

# Clean Used Cars Columns
used_cars_df = (
    used_cars_df
    .dropDuplicates()
    .na.fill({
        "brand": "unknown",
        "model": "unknown",
        "fueltype": "unknown",
        "transmission": "unknown",
        "condition": "unknown",
        "price": 0.0,
        "mileage": 0,
        "enginesize": "unknown",
        "year": 0
    })
    .withColumn("fueltype", lower(trim(col("fueltype"))))
)

# Add Unique IDs
used_cars_df = used_cars_df.withColumn("used_car_id", monotonically_increasing_id())
new_car_df = new_car_df.withColumn("new_car_id", monotonically_increasing_id())

# Fill Nulls for Remaining Numeric Columns
numeric_cols_new = [c for c, t in new_car_df.dtypes if t in ["int", "double", "float", "long"]]
string_cols_new = [c for c, t in new_car_df.dtypes if t == "string"]

new_car_df = (
    new_car_df
    .fillna(0, subset=numeric_cols_new)
    .fillna("unknown", subset=string_cols_new)
)

# Display Cleaned Tables
display(used_cars_df)
display(new_car_df)

# Finally write to silver tables
used_cars_df.write.mode("overwrite").format("delta").save(uri + "silver/used_cars")
new_car_df.write.mode("overwrite").format("delta").save(uri + "silver/new_cars")
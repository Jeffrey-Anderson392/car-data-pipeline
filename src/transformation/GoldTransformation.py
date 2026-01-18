"""This file will be used for transforming the datasets into Fact and dimension tables for Power BI"""

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
from pyspark.sql.functions import col, sha2, concat_ws, current_date
from pyspark.sql.types import IntegerType, DoubleType

""" read in tables from silver folder"""
used_cars_df = spark.read.format("delta").load(uri + "silver/used_cars")
new_car_df  = spark.read.format("delta").load(uri + "silver/new_cars")

""" dimension tables"""
# Make Dimension
dim_make = (
    used_cars_df.select(col("brand").alias("make"))
    .union(new_car_df.select(col("companyname").alias("make")))
    .distinct()
    .withColumnRenamed("make", "make_name")
    .withColumn("make_id", sha2(col("make_name"), 256))   # stable surrogate key
)

# Model Dimension
dim_model = (
    used_cars_df.select(col("brand").alias("make"), col("model"))
    .union(new_car_df.select(col("companyname").alias("make"), col("carname").alias("model")))
    .distinct()
    .withColumnRenamed("make", "make_name")
    .withColumnRenamed("model", "model_name")
    .withColumn("model_id", sha2(concat_ws(":", col("make_name"), col("model_name")), 256))  # stable surrogate key
)

""" Fact tables """
# Used Cars Fact Table
fact_used_cars = (
    used_cars_df.alias("uc")
    .join(dim_make.alias("mk"), col("uc.brand") == col("mk.make_name"), "left")
    .join(dim_model.alias("m"),
          (col("uc.brand") == col("m.make_name")) & (col("uc.model") == col("m.model_name")),
          "left"
    )
    .select(
        col("uc.carid").alias("used_car_id"),
        col("mk.make_id"),
        col("m.model_id"),
        col("uc.year"),
        col("uc.enginesize"),
        col("uc.fueltype"),
        col("uc.transmission"),
        col("uc.mileage"),
        col("uc.condition"),
        col("uc.price").alias("price_usd"),
        current_date().alias("etl_load_date")
    )
)

# New Cars Fact Table
fact_new_cars = (
    new_car_df.alias("nc")
    .join(dim_make.alias("mk"), col("nc.companyname") == col("mk.make_name"), "left")
    .join(dim_model.alias("m"),
          (col("nc.companyname") == col("m.make_name")) & (col("nc.carname") == col("m.model_name")),
          "left"
    )
    .select(
        sha2(concat_ws(":", col("nc.companyname"), col("nc.carname")), 256).alias("new_car_id"),
        col("mk.make_id"),
        col("m.model_id"),
        col("nc.engines").alias("engine_type"),
        col("nc.batterycapacity").alias("battery_capacity"),
        col("nc.horsepower"),
        col("nc.totalspeed").alias("total_speed"),
        col("nc.performance"),
        col("nc.carprice").alias("price_usd"),
        col("nc.fueltype"),
        col("nc.seats"),
        col("nc.torque"),
        current_date().alias("etl_load_date")
    )
)

""" write to gold layer"""
dim_make.write.format("delta").mode("overwrite").save(uri + "gold/dim_make")
dim_model.write.format("delta").mode("overwrite").save(uri + "gold/dim_model")
fact_used_cars.write.format("delta").mode("overwrite").save(uri + "gold/fact_used_cars")
fact_new_cars.write.format("delta").mode("overwrite").save(uri + "gold/fact_new_cars")

dim_make.write.option("header", True).mode("overwrite").csv(uri + "gold_csv/dim_make")
dim_model.write.option("header", True).mode("overwrite").csv(uri + "gold_csv/dim_model")
fact_used_cars.write.option("header", True).mode("overwrite").csv(uri + "gold_csv/fact_used_cars")
fact_new_cars.write.option("header", True).mode("overwrite").csv(uri + "gold_csv/fact_new_cars")

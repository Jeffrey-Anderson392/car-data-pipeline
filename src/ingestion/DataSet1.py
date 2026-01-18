# DataSet1.py - script to extract data from its source and load into ADLS.
"""This dataset file will be used for my used cars dataset"""

# setup the uri to pull from my storage account
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

# read in data from csv in project directory in container
from pyspark.sql import functions 

# define path to csv file
csv_path = uri + "dataset_files/car_price_prediction_.csv"

# read into spark dataframe and preview it
df_sp = spark.read.csv(csv_path, header=True, inferSchema=True)
display(df_sp)

# need to fix columns where there are illegal characters in column names
df_sp = df_sp.withColumnRenamed("Car ID", "CarID")\
              .withColumnRenamed("Engine Size", "EngineSize")\
              .withColumnRenamed("Fuel Type", "FuelType")

# display those columns to make sure they are renamed
display(df_sp[["CarID", "EngineSize", "FuelType"]])

# write data into bronze layer, path = bronze/used_cars
df_sp.write.format("delta").save(uri + "bronze/used_cars")

print("DataSet1 ingestion completed")

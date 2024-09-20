import pyspark
from delta import *
import os

BRONZE_PATH = "./VED-lakehouse/bronze/"
SILVER_PATH = "./VED-lakehouse/silver/"
GOLD_PATH = "./VED-lakehouse/gold/"
NUMERIC_COLUMNS = (
    'vehicle_speed', 'maf', 'absolute_load', 'oat',
    'fuel_rate', 'air_conditioning_power_kw', 'air_conditioning_power_watts',
    'heater_power', 'hv_battery_current', 'hv_battery_soc', 'hv_battery_voltage',
    'short_term_fuel_trim_bank_1', 'short_term_fuel_trim_bank_2',
    'long_term_fuel_trim_bank_1', 'long_term_fuel_trim_bank_2'
)

def standardize_for_delta_columns(column: str) -> str:
    if 'Air' in column:
        column = column.replace('[', '_')[:-1]
    else:
        column = column.split('(')[0].split('[')[0]

    if 'ngine' in column:
        if 'ype' in column:
            column = "vehicle_type"
        else:    
            column = "engine_configuration"
        
    
    column = column.replace(' ', '_').lower()

    return(column)

def config() -> pyspark.sql.session.SparkSession:
    builder = pyspark.sql.SparkSession.builder.appName("DataLakehouse") \
    .config("spark.master", "local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.memory.offHeap.enabled","true") \
    .config("spark.memory.offHeap.size","50g") \
    .config("spark.executor.memory", "50g") \
    .config("spark.driver.memory", "50g")     

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

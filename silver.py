from pyspark.sql.functions import col, isnan, sum as _sum, min as _min, max as _max, when, lit
from delta import *
import os
from tqdm import tqdm
import numpy as np

from config import *




def build_silver():
    spark = config()
    
    if not os.path.exists(f'{SILVER_PATH}vehicle_data'):
        static1 = spark.read.format('delta').option("header", True).load(f'{BRONZE_PATH}VED_Static_Data_ICE&HEV')
        static2 = spark.read.format('delta').option("header", True).load(f'{BRONZE_PATH}VED_Static_Data_PHEV&EV')
        
        vehicle_data = static1.union(static2)
        for column in vehicle_data.columns:
            vehicle_data = vehicle_data.withColumn(column, when(col(column) == "NO DATA", np.nan).otherwise(col(column)))
        
        vehicle_data.write.format('delta').mode("overwrite").save(f'{SILVER_PATH}vehicle_data')
    else:
        vehicle_data = spark.read.format('delta').option("header", True).load(f'{SILVER_PATH}vehicle_data')

    bronze_arch = os.listdir(BRONZE_PATH)

    table = None
    for i, num in enumerate(bronze_arch):
        if not os.path.exists(f'{SILVER_PATH}{num}/') and "Static" not in num:
            df = spark.read.format('delta').option("header", True).load(f'{BRONZE_PATH}{num}')

            columns = []
            for column in df.columns:
                columns.append(standardize_for_delta_columns(column))

            for column in NUMERIC_COLUMNS:
                df = df.withColumn(column, col(column).cast('float'))

            df = df.dropna(how='all')
            df.drop('air_conditioning_power_watts')

            df = df.dropDuplicates()
            
            filtered_df = df.filter(~isnan(col(column)))

            for column in NUMERIC_COLUMNS:
                min_value = filtered_df.agg(_min(col(column))).collect()[0][0]
                max_value = filtered_df.agg(_max(col(column))).collect()[0][0]

                columns = columns
                if max_value != min_value: 
                    normalized_col = (col(column) - min_value) / (max_value - min_value)
                    col_name = f"{column}_norm"
                    df = df.withColumn(col_name, normalized_col)
                    columns.append(col_name)
            
            

            df = df.toDF(*columns)

            df = df.withColumn("week", lit(int(num)))

            if i == 0:
                table = df
            else:
                table.union(df)

    table.join(vehicle_data, on='vehid', how='right')

    table.write.format('delta').mode("overwrite").save(f'{SILVER_PATH}VED')

    
    
        

    

build_silver()


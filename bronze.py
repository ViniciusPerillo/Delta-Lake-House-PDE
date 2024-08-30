import pandas as pd
from delta import *
import os

from config import *


def _make_folders():
    if not os.path.exists('VED-lakehouse/'): 
        os.mkdir('VED-lakehouse/')
    if not os.path.exists(BRONZE_PATH): 
        os.mkdir(BRONZE_PATH)
    if not os.path.exists(SILVER_PATH): 
        os.mkdir(SILVER_PATH)
    if not os.path.exists(GOLD_PATH): 
        os.mkdir(GOLD_PATH)

def build_bronze():
    spark = config()
    _make_folders()

    csvs = os.listdir('data/dynamic')

    for csv in csvs:
        if not os.path.exists(f'VED-lakehouse/bronze/{csv[4:-9]}/'):
            df = spark.read.format("csv").option("header", True).load(f"./data/dynamic/{csv}")
            
            columns = []
            for column in df.columns:
                columns.append(standardize_for_delta_columns(column))

            df = df.toDF(*columns)
            
            df.write.format("delta").save(f"{BRONZE_PATH}{csv[4:-9]}")

    xlsxs = os.listdir('data/static')
    
    for xlsx in xlsxs:
        if not os.path.exists(f'VED-lakehouse/bronze/{xlsx}/'):
            pd_df = pd.read_excel(f"./data/static/{xlsx}")
            df = spark.createDataFrame(pd_df)
            
            columns = []
            for column in df.columns:
                columns.append(standardize_for_delta_columns(column))

            df = df.toDF(*columns)
            
            df.write.format("delta").save(f"{BRONZE_PATH}{xlsx[:-5]}")


build_bronze()
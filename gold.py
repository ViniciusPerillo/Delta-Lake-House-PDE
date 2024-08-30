from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, isnan, sum as _sum, min as _min, max as _max, when, lit
from delta import *
import os

from config import *


def add_surrogate_key(fact: DataFrame, key: str) -> DataFrame:
    return fact.withColumn(f"sk_{key}", monotonically_increasing_id())

def create_dim_table(spark: SparkSession, fact: DataFrame, key: str) -> tuple[DataFrame, DataFrame]:
    dim = spark.createDataFrame(fact.select(key).distinct().collect(), [key])
    dim = add_surrogate_key(dim, key)
    new_fact = fact.join(dim, on= key, how='left').drop(key)
    return dim, new_fact

def create_agg_view(fact: DataFrame, dim: DataFrame, key: str):
    view = fact.join(dim, fact[f"sk_{key}"] == dim[f"sk_{key}"], how='left')
    columns_to_sum = [_sum(col).alias(col) for col in view.columns if col in NUMERIC_COLUMNS]
    view = view.select(list(NUMERIC_COLUMNS) + [key])
    view = view.groupBy(key).agg(*columns_to_sum)
    return view

def build_gold():

    spark = config()

    fact = spark.read.format('delta').option('header', True).load(f'{SILVER_PATH}VED')

    dims = ['vehid', 'trip', 'week']

    for key in dims:
        dim, fact = create_dim_table(spark, fact, key)
        
        fact.show()
        view = create_agg_view(fact, dim, key)

        dim.show()
        dim.write.format('delta').mode('overwrite').save(f'{GOLD_PATH}dim_{key}')
        
        view.show()
        view.write.format('delta').mode('overwrite').save(f'{GOLD_PATH}view_{key}')

    fact.show()
    fact.write.format('delta').mode('overwrite').save(f'{GOLD_PATH}fact')

build_gold()
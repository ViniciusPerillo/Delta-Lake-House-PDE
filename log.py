from config import *
import os

def print_delta():
    spark = config()

    for med in (BRONZE_PATH, SILVER_PATH, GOLD_PATH):
        
        for delta in os.listdir(med):
            
            print(f'Tabela {delta} donível {med}')
            spark.read.format('delta').option('header', True).load(f'{med}{delta}').show()
            print('\n\n')

        print('\n\n\n\n\n')

print_delta()
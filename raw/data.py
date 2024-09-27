#import pandas as pd

import time
from pathlib import Path


from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, StructType, StringType

class RawDataProcessor():
    
    """Reads the xlsx reports in the reports directory and correct for any duplicates"""
    
    def createDataframe(self) -> DataFrame:

        conf = SparkConf().setAppName("MapFemRS").setMaster("local").set("spark.executor.memory", "2g")
        spark = SparkSession.Builder().config(conf=conf).getOrCreate()
        empty_RDD = spark.sparkContext.emptyRDD()
        
        schema = StructType([
            StructField("Cidade", StringType(), True),
            StructField("Ano", StringType(), True),
            StructField("Fem_Ten", StringType(), True),
            StructField("Fem_Cons", StringType(), True),
            StructField("Ameaca", StringType(), True),
            StructField("Estupro", StringType(), True),
            StructField("Les_Corp", StringType(), True),
        ])

        df = spark.createDataFrame(empty_RDD, schema)

        return df

    def processData(self, path_to_file: Path, df: DataFrame): 

        file = f'{path_to_file}/report_2021.xlsx'
        sheet_names = ['Feminicídio Tentado', 'Feminicídio Consumado', 'Ameaça', 'Estupro', 'Lesão Corporal']

        for sheet in sheet_names:

            df = .read_excel(file, sheet_name=sheet, skiprows = 3, nrows = 497, )
            df = df.rename(columns={'Unnamed: 1':'CIDADES', 'Unnamed: 2':'JAN',
                                    'Unnamed: 3':'FEV', 'Unnamed: 4':'MAR',
                                    'Unnamed: 5':'ABR', 'Unnamed: 6':'MAI',
                                    'Unnamed: 7':'JUN', 'Unnamed: 8':'JUL',
                                    'Unnamed: 9':'AGO', 'Unnamed: 10':'SEP',
                                    'Unnamed: 11':'OUT', 'Unnamed: 12':'NOV',
                                    'Unnamed: 13':'DEZ', 'Unnamed: 14':'TOTAL'})
            df = df.drop(columns=['Unnamed: 0'])
            print(df.head())

    def main(self):
        RawDataProcessor.convert_to_dataframe(self, Path(f'{Path.home()}/.mapfem/data/raw'))





import time
from pathlib import Path

import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

class RawDataProcessor():
    
    """Reads the xlsx reports in the reports directory and correct for any duplicates""" 

    def processData(self, path_to_file: Path) -> dict[str, DataFrame]: 

        #Spark info
        conf = SparkConf().setAppName("MapFemRS").setMaster("local").set("spark.executor.memory", "2g")
        spark = SparkSession.Builder().config(conf=conf).getOrCreate()
        


        file = str(path_to_file) + "/2019.xlsx"

        files = list(path_to_file.iterdir())
        sheet_names = ['Feminicídio Tentado', 'Feminicídio Consumado', 'Ameaça', 'Estupro', 'Lesão Corporal']
        
        files_dataframes = dict()

        for file in files:
            print(f"{file.stem} ----------------------------------------------------------------------------------------------------------------- ")

            file_df = None
            for sheet in sheet_names:
                
                if file.stem == "2017":
                    continue

                df = pd.read_excel(file, engine="openpyxl", sheet_name=sheet, skiprows = 3, nrows = 497,  )

                df = spark.createDataFrame(df)
                df = df.drop("Unnamed: 0")
                
                df = df.withColumnRenamed(f"{df.columns[0]}", "Cidades") \
                        .withColumnRenamed(f"{df.columns[1]}", "JAN") \
                        .withColumnRenamed(f"{df.columns[2]}", "FEV") \
                        .withColumnRenamed(f"{df.columns[3]}", "MAR") \
                        .withColumnRenamed(f"{df.columns[4]}", "ABR") \
                        .withColumnRenamed(f"{df.columns[5]}", "MAI") \
                        .withColumnRenamed(f"{df.columns[6]}", "JUN") \
                        .withColumnRenamed(f"{df.columns[7]}", "JUL") \
                        .withColumnRenamed(f"{df.columns[8]}", "AGO") \
                        .withColumnRenamed(f"{df.columns[9]}", "SEP") \
                        .withColumnRenamed(f"{df.columns[10]}", "OUT") \
                        .withColumnRenamed(f"{df.columns[11]}", "NOV") \
                        .withColumnRenamed(f"{df.columns[12]}", "DEZ") \
                        .withColumnRenamed(f"{df.columns[13]}", "TOTAL")
                

                if file_df is None:
                    file_df = df
                else: 
                    file_df = file_df.join(other=df, on="Cidades", how="inner")

            files_dataframes[file.stem] = file_df
        
        print(files_dataframes["2018"].show())
        return files_dataframes
            
    def transformData(self):
        return

    def main(self):
        RawDataProcessor.processData(self, Path(f'{Path.home()}/.mapfem/data/raw'))





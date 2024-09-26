import pandas as pd
import time
from pathlib import Path


class RawData():
    """Reads the xlsx reports in the reports directory and correct for any duplicates"""
    


    def convert_to_dataframe(self, path_to_file: Path): 
        
        file = f'{path_to_file}/report_2021.xlsx'
        sheet_names = ['Feminicídio Tentado', 'Feminicídio Consumado', 'Ameaça', 'Estupro', 'Lesão Corporal']
        
        for sheet in sheet_names:
            df = pd.read_excel(file, sheet_name=sheet, skiprows = 3, nrows = 497, )
            df = df.rename(columns={'Unnamed: 1':'CIDADES', 'Unnamed: 2':'JAN',
                                'Unnamed: 3':'FEV', 'Unnamed: 4':'MAR',
                                'Unnamed: 5':'ABR', 'Unnamed: 6':'MAI',
                                'Unnamed: 7':'JUN', 'Unnamed: 8':'JUL',
                                'Unnamed: 9':'AGO', 'Unnamed: 10':'SEP',
                                'Unnamed: 11':'OUT', 'Unnamed: 12':'NOV',
                                'Unnamed: 13':'DEZ', 'Unnamed: 14':'TOTAL'})
            df = df.drop(columns=['Unnamed: 0'])
            print(df.head())
                

   #     files_df = []
   #     print('Creating Dataframes \n')
   #     print('---------------------------------------------------------------------\n')
   #     time.sleep(2)

   #     for file in path_to_file.rglob('*.xlsx'):

   #         if(file.name == 'report_2017.xlsx'):
   #             #TODO: function should be called
   #             continue
   #         
   #         for sheet in sheet_names:
   #             print(f'Dataframe {sheet} created from {file.name}\n')

   #             df = pd.read_excel(file, sheet_name=sheet, skiprows = 3, nrows = 497, )
   #             df = df.rename(columns={'Unnamed: 1':'CIDADES', 'Unnamed: 2':'JAN',
   #                             'Unnamed: 3':'FEV', 'Unnamed: 4':'MAR',
   #                             'Unnamed: 5':'ABR', 'Unnamed: 6':'MAI',
   #                             'Unnamed: 7':'JUN', 'Unnamed: 8':'JUL',
   #                             'Unnamed: 9':'AGO', 'Unnamed: 10':'SEP',
   #                             'Unnamed: 11':'OUT', 'Unnamed: 12':'NOV',
   #                             'Unnamed: 13':'DEZ', 'Unnamed: 14':'TOTAL'})
   #             df = df.drop(columns=['Unnamed: 0'])
   #             
   #             
   #             


   #         print(f'Dataframe {file.name} created succcesfully!\n') 
   #         print('----------------------------------------------\n')

   #         time.sleep(0.5)


   #     print('Finish!\n')

   #     print(files_df[0].head(10))
   #     return files_df

    def main(self):
        RawData.convert_to_dataframe(self, Path(f'{Path.home()}/.mapfem/data/raw'))
        




import pandas as pd
from pathlib import Path


class RawData():
    """Reads the xlsx reports in the reports directory and correct for any duplicates"""

    def convert_to_dataframe(self, path_to_file: Path) -> pd.DataFrame: 
        
        excel_df = pd.read_excel(path_to_file, sheet_name='Lesão Corporal')
        
        df = excel_df.drop(columns=['Unnamed: 0']).drop([0,1])
        
        df = df.rename(columns={'Unnamed: 1':'CIDADES', 'Unnamed: 2':'JAN',
                                'Unnamed: 3':'FEV', 'Unnamed: 4':'MAR',
                                'Unnamed: 5':'ABR', 'Unnamed: 6':'MAI',
                                'Unnamed: 7':'JUN', 'Unnamed: 8':'JUL',
                                'Unnamed: 9':'AGO', 'Unnamed: 10':'SEP',
                                'Unnamed: 11':'OUT', 'Unnamed: 12':'NOV',
                                'Unnamed: 13':'DEZ', 'Unnamed: 14':'TOTAL'})
        print(df.head(50))
        
        return excel_df


    def main(self):
        RawData.convert_to_dataframe(self, Path(f'{Path.home()}/.mapfem/data/raw/report_2023.xlsx'))





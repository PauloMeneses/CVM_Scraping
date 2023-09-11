import os
import wget
import pandas as pd
from zipfile import ZipFile
import datetime

def download_files(year_range):
    """
    Download all files from CVM website. The files are downloaded in the current directory. 

    Args:
        year_range (tuple): A tuple with the first and last year to download.
    Returns:
        None

    """

    base_url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/' 
    current_year = datetime.datetime.now().year

    for year in range(year_range[0], min(year_range[1], current_year + 1)):
        zip_file_name = f'itr_cia_aberta_{year}.zip'
        wget.download(base_url + zip_file_name)
        with ZipFile(zip_file_name, 'r') as zipObj:
            zipObj.extractall('CVM')
        os.remove(zip_file_name)

def process_indicators(indicator_names, year_range):
    """
    Process all indicators and save them in parquet format. 

    Args:
        indicator_names (list): A list with the indicators names.
        year_range (tuple): A tuple with the first and last year to download.

    Returns:
        None        
    
    """
    for indicator in indicator_names:
        indicator_df = pd.DataFrame()
        for year in range(year_range[0], min(year_range[1], datetime.datetime.now().year + 1)):
            csv_file_name = f'CVM/itr_cia_aberta_{indicator}_{year}.csv'
            if os.path.exists(csv_file_name):
                indicator_df = pd.concat([indicator_df, pd.read_csv(csv_file_name, sep=';', decimal=',', encoding='ISO-8859-1')])
        indicator_folder = os.path.join('CVM', indicator)
        if not os.path.exists(indicator_folder):
            os.mkdir(indicator_folder)
        parquet_file_path = os.path.join(indicator_folder, f'{indicator}_2011_{min(year_range[1], datetime.datetime.now().year)}.parquet')
        indicator_df.to_parquet(parquet_file_path, index=False)

def process_cia_aberta_data(year_range):
    """ 
    Process the cia_aberta data and save it in parquet format.

    Args: year_range (tuple): A tuple with the first and last year to download.
    
    Returns: None
    """
    cia_aberta_df = pd.DataFrame()
    for year in range(year_range[0], min(year_range[1], datetime.datetime.now().year + 1)):
        csv_file_name = f'CVM/itr_cia_aberta_{year}.csv'
        if os.path.exists(csv_file_name):
            cia_aberta_df = pd.concat([cia_aberta_df, pd.read_csv(csv_file_name, sep=';', decimal=',', encoding='ISO-8859-1')])
    cia_aberta_folder = os.path.join('CVM', 'cia_aberta')
    if not os.path.exists(cia_aberta_folder):
        os.mkdir(cia_aberta_folder)
    parquet_file_path = os.path.join(cia_aberta_folder, f'itr_cia_aberta_{year_range[0]}_{min(year_range[1], datetime.datetime.now().year)}.parquet')
    cia_aberta_df.to_parquet(parquet_file_path, index=False)

def main():
    year_range = (2011, datetime.datetime.now().year)
    download_files(year_range)

    indicator_names = [
        'BPA_con', 'BPA_ind', 'BPP_con', 'BPP_ind',
        'DFC_MD_con', 'DFC_MD_ind', 'DFC_MI_con', 'DFC_MI_ind',
        'DMPL_con', 'DMPL_ind', 'DRA_con', 'DRA_ind',
        'DRE_con', 'DRE_ind', 'DVA_con', 'DVA_ind'
    ]
    process_indicators(indicator_names, year_range)

    process_cia_aberta_data(year_range)

    # Limpeza: apagar arquivos CSV
    for file_name in os.listdir('CVM'):
        if file_name.endswith('.csv'):
            os.remove(os.path.join('CVM', file_name))

if __name__ == "__main__":
    main()

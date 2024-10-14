import csv
import re
import pandas as pd
import numpy as np
import logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S',  
                    handlers=[
                        logging.StreamHandler()  
                    ])

logger = logging.getLogger(__name__)  
def cleaning_data(input_file='./data/movies.csv', output_file='./data/clean_movies.csv', index_to_update=844):
    """
    Fungsi ini untuk membersihkan data dari csv yang formatnya rusak
    Mengganti String yang hanya memiliki satu double quote(") menjadi dua(awal dan akhir dari stringnya)
    Ada data title yang tidak terpengaruh sehingga kita mengubahnya secara manual
    """
    logger.info("Cleaning data")
    #Mengubah secara manual
    new_row_data = [
        "Nativity 3: Dude Where's My Donkey?!",  # Title
        3.6,                                     # Rating
        2014,                                    # Year
        "November",                              # Month
        "",                                      # Certificate
        109,                                     # Runtime
        "Debbie Isitt",                          # Directors
        '"Martin Clunes, Marc Wootton, Catherine Tate, Adam Garcia"',  # Stars
        '"Comedy, Family"',                      # Genre
        "UK",                                    # Filming Location
        "Unknown",                               # Budget
        '"$11,283,866 "',                        # Income
        "United Kingdom"                         # Country of Origin
    ]

    fixed_rows = []
    # Mengubah data dengan double quote
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)

        headers = next(reader) 
        fixed_rows.append(headers) 
        
        for row in reader:
            if len(row) == 1: 
                fixed_row = re.findall(r'"[^"]*"|[^,]+', row[0]) #Semua character kecuali double quote (")
                #Jika ada record yang memiliki (,) maka akan ditambahkan double quote didalamnya 
                fixed_row = [item.strip('"') if ',' not in item else item for item in fixed_row] 
                
                if len(fixed_row) != len(headers):
                    # Jika jumlah kolom tidak sama, bisa jadi ada kesalahan dalam format data.
                    continue  
                fixed_rows.append(fixed_row)  
            else:
                fixed_rows.append(row)  

    df = pd.DataFrame(fixed_rows[1:], columns=fixed_rows[0])  

    df.loc[index_to_update] = new_row_data
 
    # Menfilter hanya nomor saja yang ada
    for column in ['Budget', 'Income']:
        df[column] = df[column].replace(r'[^0-9.]', '', regex=True)  # Remove all non-numeric characters
        df[column] = pd.to_numeric(df[column], errors='coerce')
   
    #Jika ada Null Value maka akan diganti 0
    df['Budget'] = df['Budget'].fillna(0)  
    df['Income'] = df['Income'].fillna(0)

    #Menghapus kelebihan double quote pada string  
    df['Country_of_origin'] = df['Country_of_origin'].str.replace('"', '', regex=False)
    df['Directors'] = df['Directors'].str.replace('"', '', regex=False)
    df['Stars'] = df['Stars'].str.replace('"', '', regex=False)
    
    
    #Menambahkan release_date yang berisi year dan month
    month_mapping = {
    'January': '01',
    'February': '02',
    'March': '03',
    'April': '04',
    'May': '05',
    'June': '06',
    'July': '07',
    'August': '08',
    'September': '09',
    'October': '10',
    'November': '11',
    'December': '12'
    }
    df['release_date'] = df['Year'].astype(str) + '-' + df['Month'].map(month_mapping)

    #Ada invalid bulan mengecek dengan ini jika ada bulan yang tidak sesuai dengan list ini akan NaN
    valid_months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
    df['Month'] = df['Month'].apply(lambda x: x if x in valid_months else np.nan)  # or 'Unknown'

    #Mengubah nama column menjadi lowercase
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    #Save to csv
    df.to_csv(output_file, index=False)

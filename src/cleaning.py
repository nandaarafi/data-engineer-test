import csv
import re
import pandas as pd

def cleaning_data(input_file='../data/movies.csv', output_file='../data/clean_movies.csv', index_to_update=844):
    """
    Fungsi ini untuk membersihkan data dari csv yang formatnya rusak
    Mengganti String yang hanya memiliki satu double quote(") menjadi dua(awal dan akhir dari stringnya)
    Ada data title yang tidak terpengaruh sehingga kita mengubahnya secara manual
    """
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

    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)

        headers = next(reader) 
        fixed_rows.append(headers) 
        
        for row in reader:
            if len(row) == 1: 
                fixed_row = re.findall(r'"[^"]*"|[^,]+', row[0])
                #Jika ada record yang memiliki (,) maka akan ditambahkan double quote didalamnya 
                fixed_row = [item.strip('"') if ',' not in item else item for item in fixed_row] 
                
                if len(fixed_row) != len(headers):
                    print(f"Warning: Row has {len(fixed_row)} columns instead of {len(headers)}.")
                    print(f"Row data: {fixed_row}")
                    continue  
                fixed_rows.append(fixed_row)  
                fixed_rows.append(row)  

    df = pd.DataFrame(fixed_rows[1:], columns=fixed_rows[0])  

    df.loc[index_to_update] = new_row_data

    df.to_csv(output_file, index=False)
    print(f"Updated CSV has been saved to {output_file}.")
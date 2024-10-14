import pandas as pd

# Fungsi untuk membersihkan dan memproses data dari file CSV
def process_data(file_path="./data/clean_movies.csv"):
    df = pd.read_csv(file_path)

    df.columns = df.columns.str.strip()

    df['Budget'] = df['Budget'].replace({'\$': '', ',': ''}, regex=True).astype(float)
    # df['Income'] = df['Income'].replace({'\$': '', ',': ''}, regex=True).astype(float)

    # df['Directors'] = df['Directors'].str.split(', ')
    # df['Stars'] = df['Stars'].str.split(', ')

    # df['Month'] = df['Month'].str.strip()
    # df['Year'] = df['Year'].astype(int)

    # df = df.fillna('Unknown')

    # print("Preview data setelah diproses:")
    # print(df.head())

    return df

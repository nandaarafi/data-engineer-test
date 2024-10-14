from src.database import connect_db, create_tables, insert_data
from src.cleaning import cleaning_data
from src.queries import *
import pandas as pd

def main():
    
    cleaning_data()
    
    df = pd.read_csv('./data/clean_movies.csv')
    conn = connect_db()
    
    create_tables(conn)
    # # print(df)
    # # conn.close()
    insert_data(conn, df)
    
    print("Film berdasarkan sertifikasi:")
    print(get_films_by_certificate(conn))
    
    print("Sutradara dengan film rating tinggi:")
    print(get_directors_with_multiple_high_rating_films(conn))
    
    print("Film yang meraih keuntungan:")
    print(get_profitable_films(conn))
    
    print("Bulan dengan film terbanyak:")
    print(get_month_with_most_films(conn))
    
    print("Top 5 film per bulan:")
    print(get_top_5_films_per_month(conn))
    conn.close()

if __name__ == "__main__":
    main()

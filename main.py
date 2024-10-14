from src.data_processing import process_data
from src.database import connect_db, create_tables, insert_data
from src.cleaning import cleaning_data
from src.queries import get_films_by_certificate, get_directors_with_multiple_high_rating_films, get_profitable_films, get_month_with_most_films, get_top_5_films_per_month

def main():
    cleaning_data()
    
    df = process_data()
    
    conn = connect_db()
    
    create_tables(conn)
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

if __name__ == "__main__":
    main()

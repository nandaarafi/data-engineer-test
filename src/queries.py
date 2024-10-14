import duckdb
CSV_PATH='./result_data/'
def save_to_csv(conn, filename, query):
    copy_query = f"COPY ({query}) TO '{filename}' WITH (HEADER, DELIMITER ',');"
    conn.execute(copy_query)

# a. Grouping data berdasarkan sertifikasi film
def get_films_by_certificate(conn):
    query = '''
        SELECT certificate
        FROM movie
        GROUP BY certificate
    '''
    save_to_csv(conn, f'{CSV_PATH}films_by_certificate.csv', query)  # Save results to CSV
    return conn.execute(query).fetchall() 

# b. Tampilkan sutradara yang telah menyutradarai lebih dari 1 film dan film-film tersebut memiliki rating diatas 7.5
def get_directors_with_multiple_high_rating_films(conn):
    query = '''
        SELECT d.director_name, COUNT(m.title) as movie_count
        FROM directors d
        JOIN movie m ON d.movie_id = m.movie_id
        WHERE m.rating > 7.5
        GROUP BY d.director_name
        HAVING movie_count > 1
    '''
    save_to_csv(conn, f'{CSV_PATH}directors_with_multiple_high_rating_films.csv', query)  # Save results to CSV
    return conn.execute(query).fetchall() 

# c. Tampilkan film yang meraih keuntungan dalam produksinya
def get_profitable_films(conn):
    query = '''
        SELECT title, (income - budget) as profit
        FROM movie
        WHERE income > budget
    '''
    save_to_csv(conn, f'{CSV_PATH}profitable_films.csv', query)  # Save results to CSV
    return conn.execute(query).fetchall()  

# d. Bulan mana yang memiliki jumlah film yang diproduksi terbanyak
def get_month_with_most_films(conn):
    query = '''
        SELECT month, COUNT(*) as movie_count
        FROM movie
        GROUP BY month
        ORDER BY movie_count DESC
    '''
    save_to_csv(conn, f'{CSV_PATH}month_with_most_films.csv', query)  # Save results to CSV
    return conn.execute(query).fetchall()  

# e. Sebutkan 5 film dengan rating tertinggi per bulan (urutkan dari rating yang terkecil ke yang terbesar)
def get_top_5_films_per_month(conn):
    query = '''
        SELECT title, rating, month, movie_id
        FROM movie
        ORDER BY month, rating DESC
    '''
    top_5_query = f"SELECT * FROM ({query}) LIMIT 5"
    save_to_csv(conn, f'{CSV_PATH}top_5_films_per_month.csv', top_5_query)  # Save results to CSV
    return conn.execute(top_5_query).fetchall()  
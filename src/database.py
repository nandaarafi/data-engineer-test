import duckdb

# Fungsi untuk menghubungkan ke DuckDB
def connect_db(db_path='./db/movies.duckdb'):
    conn = duckdb.connect(db_path)
    return conn

# Fungsi untuk membuat tabel (jika perlu, meskipun DuckDB bisa langsung load CSV)
def create_tables(conn):
    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS film (
            id_film INTEGER PRIMARY KEY,  
            Title TEXT,
            Rating REAL,
            Year INTEGER,
            Month TEXT,
            Certificate TEXT,
            Runtime INTEGER,
            Filming_location TEXT,
            Budget REAL,
            Income REAL,
            Country_of_origin TEXT
        )
    ''')
    
    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS sutradara_film (
            id_sutradara INTEGER PRIMARY KEY,  
            id_film INTEGER,
            nama_sutradara TEXT
        )
    ''')
    
    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS pemeran_film (
            id_pemeran INTEGER PRIMARY KEY,  
            id_film INTEGER,
            nama_pemeran TEXT
        )
    ''')

    # Create sequences for each table
    conn.execute("CREATE SEQUENCE film_seq START WITH 1 INCREMENT BY 1;")
    conn.execute("CREATE SEQUENCE sutradara_film_seq START WITH 1 INCREMENT BY 1;")
    conn.execute("CREATE SEQUENCE pemeran_film_seq START WITH 1 INCREMENT BY 1;")

# Fungsi untuk memasukkan data ke DuckDB
def insert_data(conn, df):
    # Masukkan data ke tabel film
    df_film = df[['Title', 'Rating', 'Year', 'Month', 'Certificate', 'Runtime', 'Filming_location', 'Budget', 'Income', 'Country_of_origin']]

    # Insert data into the film table using a DataFrame
    for index, row in df_film.iterrows():
        conn.execute('''
            INSERT INTO film (id_film, Title, Rating, Year, Month, Certificate, Runtime, Filming_location, Budget, Income, Country_of_origin)
            VALUES (nextval('film_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['Title'], row['Rating'], row['Year'], row['Month'], row['Certificate'], row['Runtime'], row['Filming_location'], row['Budget'], row['Income'], row['Country_of_origin']))

    # Masukkan data ke tabel sutradara
    for index, row in df.iterrows():
        directors = row['Directors'].split(', ')  # Split directors if they're in a single string
        for director in directors:
            conn.execute('''
                INSERT INTO sutradara_film (id_sutradara, id_film, nama_sutradara) 
                VALUES (nextval('sutradara_film_seq'), ?, ?)
            ''', (index + 1, director))  # Assuming id_film corresponds to index + 1

    # Masukkan data ke tabel pemeran
    for index, row in df.iterrows():
        stars = row['Stars'].split(', ')  # Split stars if they're in a single string
        for star in stars:
            conn.execute('''
                INSERT INTO pemeran_film (id_pemeran, id_film, nama_pemeran) 
                VALUES (nextval('pemeran_film_seq'), ?, ?)
            ''', (index + 1, star))

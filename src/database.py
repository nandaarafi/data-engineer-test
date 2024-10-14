import duckdb
import logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
                    datefmt='%Y-%m-%d %H:%M:%S',  
                    handlers=[
                        logging.StreamHandler()  
                    ])

logger = logging.getLogger(__name__)  
def connect_db(db_path='./db/movies.duckdb'):
    conn = duckdb.connect(db_path)
    return conn

# Fungsi untuk membuat tabel
def create_tables(conn):
    logger.info("Create movie, directors, and stars table")

    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS movie (
            movie_id INTEGER PRIMARY KEY,  
            title TEXT,
            rating REAL,
            year INTEGER,
            month TEXT,
            release_date TEXT,
            certificate TEXT,
            runtime INTEGER,
            filming_location TEXT,
            budget DECIMAL(18,2),
            income DECIMAL(18,2),
            country_of_origin TEXT
        )
    ''')
    
    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS directors (
            director_id INTEGER PRIMARY KEY,  
            movie_id INTEGER,
            director_name TEXT
        )
    ''')
    
    conn.execute(''' 
        CREATE TABLE IF NOT EXISTS stars (
            star_id INTEGER PRIMARY KEY,  
            movie_id INTEGER,
            star_name TEXT
        )
    ''')
    conn.execute("CREATE SEQUENCE IF NOT EXISTS movie_seq START WITH 1 INCREMENT BY 1;")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS directors_seq START WITH 1 INCREMENT BY 1;")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS stars_seq START WITH 1 INCREMENT BY 1;")


def insert_data(conn, df):
    df_movie = df[['title', 'rating', 'year', 'month', 'release_date', 'certificate', 'runtime', 'filming_location', 'budget', 'income', 'country_of_origin']]

    logger.info("Insert movie Table")


    for index, row in df_movie.iterrows():
        conn.execute('''
            INSERT INTO movie (movie_id, title, rating, year, month, release_date, certificate, runtime, filming_location, budget, income, country_of_origin)
            VALUES (nextval('movie_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (row['title'], row['rating'], row['year'], row['month'], row['release_date'], row['certificate'], row['runtime'], row['filming_location'], row['budget'], row['income'], row['country_of_origin']))

    logger.info("Insert directors tabel")
    for index, row in df.iterrows():
        directors = row['directors'].split(', ')  
        for director in directors:
            conn.execute('''
                INSERT INTO directors (director_id, movie_id, director_name) 
                VALUES (nextval('directors_seq'), nextval('movie_seq'), ?)
            ''', [director])  

    logger.info("Insert stars tabel")

    for index, row in df.iterrows():
        stars = row['stars'].split(', ')  
        for star in stars:
            conn.execute('''
                INSERT INTO stars (star_id, movie_id, star_name) 
                VALUES (nextval('stars_seq'), nextval('movie_seq'), ?)
            ''', [star])



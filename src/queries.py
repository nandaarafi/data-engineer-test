def get_films_by_certificate(conn):
    query = '''
        SELECT Certificate, COUNT(*) as jumlah_film
        FROM film
        GROUP BY Certificate
    '''
    return conn.execute(query).fetchall()

def get_directors_with_multiple_high_rating_films(conn):
    query = '''
        SELECT sf.nama_sutradara, COUNT(f.Title) as jumlah_film
        FROM sutradara_film sf
        JOIN film f ON sf.id_film = f.id_film
        WHERE f.Rating > 7.5
        GROUP BY sf.nama_sutradara
        HAVING jumlah_film > 1
    '''
    return conn.execute(query).fetchall()

def get_profitable_films(conn):
    query = '''
        SELECT Title, (Income - Budget) as profit
        FROM film
        WHERE Income > Budget
    '''
    return conn.execute(query).fetchall()

def get_month_with_most_films(conn):
    query = '''
        SELECT Month, COUNT(*) as jumlah_film
        FROM film
        GROUP BY Month
        ORDER BY jumlah_film DESC
    '''
    return conn.execute(query).fetchall()

def get_top_5_films_per_month(conn):
    query = '''
        SELECT Title, Rating
        FROM film
        ORDER BY Month, Rating DESC
        LIMIT 5
    '''
    return conn.execute(query).fetchall()

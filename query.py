import psycopg2

class Database:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()

    def execute_query(self, query, data=None):
        self.cur.execute(query, data)

    def fetch_all(self):
        return self.cur.fetchall()

    def commit_and_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

class UserQueries:
    def __init__(self, db):
        self.db = db

    def create_user(self, username, password):
        query = "INSERT INTO user (username, password) VALUES (%s, %s) RETURNING user_id"
        data = (username, password)
        self.db.execute_query(query, data)
        return self.db.cur.fetchone()[0]

    def get_user_by_username(self, username):
        query = "SELECT * FROM user WHERE username = %s"
        data = (username,)
        self.db.execute_query(query, data)
        return self.db.fetch_all()

class MovieQueries:
    def __init__(self, db):
        self.db = db

    def create_movie(self, title, genre_id, description, release_date):
        query = "INSERT INTO movie (title, genre_id, description, release_date) VALUES (%s, %s, %s, %s) RETURNING movie_id"
        data = (title, genre_id, description, release_date)
        self.db.execute_query(query, data)
        return self.db.cur.fetchone()[0]

    def get_movie_by_title(self, title):
        query = "SELECT * FROM movie WHERE title = %s"
        data = (title,)
        self.db.execute_query(query, data)
        return self.db.fetch_all()

# Create other classes and functions for different tables as needed

if __name__ == "__main__":
    db = Database("your_database_name", "your_username", "your_password", "your_host", "your_port")

    # Example usage of UserQueries
    user_queries = UserQueries(db)
    user_id = user_queries.create_user("example_user", "password123")
    print(f"Created user with ID: {user_id}")

    users = user_queries.get_user_by_username("example_user")
    print("Users with the username 'example_user':", users)

    # Example usage of MovieQueries
    movie_queries = MovieQueries(db)
    movie_id = movie_queries.create_movie("Sample Movie", 1, "A sample movie description.", "2023-01-01")
    print(f"Created movie with ID: {movie_id}")

    movies = movie_queries.get_movie_by_title("Sample Movie")
    print("Movies with the title 'Sample Movie':", movies)

    db.commit_and_close()

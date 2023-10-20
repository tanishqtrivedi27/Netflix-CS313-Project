import psycopg2
from decouple import config

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

    def create_user(self, username, password, email):
        query = "INSERT INTO user (username, password, email) VALUES (%s, %s, %s) RETURNING user_id"
        data = (username, password, email)
        self.db.execute_query(query, data)
        return self.db.cur.fetchone()[0]

    def get_user_by_email_password(self, email, password):
        query = "SELECT user_id FROM user WHERE email = %s AND password = %s"
        data = (email, password)
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
        query = "SELECT movie_id FROM movie WHERE title = %s"
        data = (title,)
        self.db.execute_query(query, data)
        return self.db.fetch_all()

class User:
    def __init__(self, username, password, email):
        self.db_name = config('DB_NAME')
        self.db_user = config('DB_USER')
        self.db_password = config('DB_PASSWORD')
        self.db_host = config('DB_HOST')
        self.db_port = config('DB_PORT')
        
        self.db = Database(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        self.user_queries = UserQueries(self.db)
        self.movie_queries = MovieQueries(self.db)

        self.username = username
        self.password = password
        self.email = email
        self.user_id = None

    def signup(self):
        # Check if the user already exists
        user_data = self.user_queries.get_user_by_email_password(self.email, self.password)
        if user_data:
            return "User already exists. Please log in."
        
        # Create the user and get the user_id
        self.user_id = self.user_queries.create_user(self.username, self.password, self.email)
        return "User created successfully."

    def login(self):
        # Check if the user exists
        user_data = self.user_queries.get_user_by_email_password(self.email, self.password)
        if not user_data:
            return "Invalid email or password."

        # Set the user_id
        self.user_id = user_data[0][0]
        return "Login successful."

    def add_movie_to_watchlist(self, title):
        # Create the movie if it doesn't exist
        movie_data = self.movie_queries.get_movie_by_title(title)
        if not movie_data:
            movie_id = self.movie_queries.create_movie(title, 1, "Description not available", "2023-01-01")
        else:
            movie_id = movie_data[0][0]

        # Add the movie to the watchlist
        watchlist_query = """
            INSERT INTO watchlist (user_id, profile_id, movie_id, rating, timestamp)
            VALUES (%s, %s, %s, %s, '00:00:00')
        """
        data = (self.user_id, 1, movie_id, "Not for me")
        self.db.execute_query(watchlist_query, data)
        self.db.commit_and_close()
        return "Movie added to watchlist."

    def update_movie_timestamp(self, title, timestamp):
        # Get the movie_id by title
        movie_data = self.movie_queries.get_movie_by_title(title)
        if not movie_data:
            return "Movie not found."

        movie_id = movie_data[0][0]

        # Update the timestamp in the watchlist
        update_timestamp_query = """
            UPDATE watchlist
            SET timestamp = %s
            WHERE user_id = %s AND profile_id = %s AND movie_id = %s
        """
        data = (timestamp, self.user_id, 1, movie_id)
        self.db.execute_query(update_timestamp_query, data)
        self.db.commit_and_close()
        return "Timestamp updated successfully."

# Example usage:
if __name__ == "__main__":
    # Create a user
    user = User("example_user", "password123", "example@example.com")
    signup_result = user.signup()
    print(signup_result)

    # Log in
    login_result = user.login()
    print(login_result)

    # Add a movie to the watchlist
    add_movie_result = user.add_movie_to_watchlist("Sample Movie")
    print(add_movie_result)

    # Update the timestamp for a movie in the watchlist
    update_timestamp_result = user.update_movie_timestamp("Sample Movie", "02:30:00")
    print(update_timestamp_result)

# Create other classes and functions for different tables as needed
class User:
    def __init__(self, username, password, email):
        self.db_name = config('DB_NAME')
        self.db_user = config('DB_USER')
        self.db_password = config('DB_PASSWORD')
        self.db_host = config('DB_HOST')
        self.db_port = config('DB_PORT')
        
        self.db = Database(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        self.user_queries = UserQueries(self.db)
        self.movie_queries = MovieQueries(self.db)

        self.username = username
        self.password = password
        self.email = email
        self.user_id = None

    def signup(self):
        # Check if the user already exists
        user_data = self.user_queries.get_user_by_email_password(self.email, self.password)
        if user_data:
            return "User already exists. Please log in."
        
        # Create the user and get the user_id
        self.user_id = self.user_queries.create_user(self.username, self.password, self.email)
        return "User created successfully."

    def login(self):
        # Check if the user exists
        user_data = self.user_queries.get_user_by_email_password(self.email, self.password)
        if not user_data:
            return "Invalid email or password."

        # Set the user_id
        self.user_id = user_data[0][0]
        return "Login successful."

    def add_movie_to_watchlist(self, title):
        # Create the movie if it doesn't exist
        movie_data = self.movie_queries.get_movie_by_title(title)
        if not movie_data:
            movie_id = self.movie_queries.create_movie(title, 1, "Description not available", "2023-01-01")
        else:
            movie_id = movie_data[0][0]

        # Add the movie to the watchlist
        watchlist_query = """
            INSERT INTO watchlist (user_id, profile_id, movie_id, rating, timestamp)
            VALUES (%s, %s, %s, %s, '00:00:00')
        """
        data = (self.user_id, 1, movie_id, "Not for me")
        self.db.execute_query(watchlist_query, data)
        self.db.commit_and_close()
        return "Movie added to watchlist."

    def update_movie_timestamp(self, title, timestamp):
        # Get the movie_id by title
        movie_data = self.movie_queries.get_movie_by_title(title)
        if not movie_data:
            return "Movie not found."

        movie_id = movie_data[0][0]

        # Update the timestamp in the watchlist
        update_timestamp_query = """
            UPDATE watchlist
            SET timestamp = %s
            WHERE user_id = %s AND profile_id = %s AND movie_id = %s
        """
        data = (timestamp, self.user_id, 1, movie_id)
        self.db.execute_query(update_timestamp_query, data)
        self.db.commit_and_close()
        return "Timestamp updated successfully."

# Example usage:
if __name__ == "__main__":
    # Create a user
    user = User("example_user", "password123", "example@example.com")
    signup_result = user.signup()
    print(signup_result)

    # Log in
    login_result = user.login()
    print(login_result)

    # Add a movie to the watchlist
    add_movie_result = user.add_movie_to_watchlist("Sample Movie")
    print(add_movie_result)

    # Update the timestamp for a movie in the watchlist
    update_timestamp_result = user.update_movie_timestamp("Sample Movie", "02:30:00")
    print(update_timestamp_result)
    
if __name__ == "__main__":
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)

    # Example usage of UserQueries
    user_queries = UserQueries(db)
    # user_id = user_queries.create_user("example_user", "password123")
    # print(f"Created user with ID: {user_id}")

    users = user_queries.get_user_by_username("example_user")
    print("Users with the username 'example_user':", users)

    # Example usage of MovieQueries
    movie_queries = MovieQueries(db)
    movie_id = movie_queries.create_movie("Sample Movie", 1, "A sample movie description.", "2023-01-01")
    print(f"Created movie with ID: {movie_id}")

    movies = movie_queries.get_movie_by_title("Sample Movie")
    print("Movies with the title 'Sample Movie':", movies)

    db.commit_and_close()

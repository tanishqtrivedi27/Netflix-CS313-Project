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

    def create_table(self, table_name, columns):
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        self.cur.execute(create_query)

    def commit_and_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

def create_user_table(db):
    columns = """
        user_id SERIAL PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL
    """
    db.create_table("user", columns)

def create_profile_table(db):
    columns = """
        profile_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        profile_name VARCHAR(255) NOT NULL
    """
    db.create_table("profile", columns)

def create_billing_table(db):
    columns = """
        billing_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        payment_info TEXT,
        active_subscription_id INT
    """
    db.create_table("billing", columns)

def create_invoice_table(db):
    columns = """
        invoice_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        invoice_details TEXT
    """
    db.create_table("invoice", columns)

def create_streaming_session_table(db):
    columns = """
        session_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        profile_id INT REFERENCES profile(profile_id),
        start_time TIMESTAMP,
        end_time TIMESTAMP
    """
    db.create_table("streaming_session", columns)

def create_subscription_tiers_table(db):
    columns = """
        tier_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        max_devices INT
    """
    db.create_table("subscription_tiers", columns)

def create_movie_table(db):
    columns = """
        movie_id SERIAL PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        genre_id INT REFERENCES genre(genre_id),
        description TEXT,
        release_date DATE
    """
    db.create_table("movie", columns)

def create_genre_table(db):
    columns = """
        genre_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL
    """
    db.create_table("genre", columns)

def create_watchlist_table(db):
    columns = """
        watchlist_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        movie_id INT REFERENCES movie(movie_id),
        timestamp TIMESTAMP
    """
    db.create_table("watchlist", columns)

def create_interaction_table(db):
    columns = """
        interaction_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        movie_id INT REFERENCES movie(movie_id),
        interaction_type VARCHAR(20),
        timestamp TIMESTAMP
    """
    db.create_table("interaction", columns)

def create_wishlist_table(db):
    columns = """
        wishlist_id SERIAL PRIMARY KEY,
        user_id INT REFERENCES user(user_id),
        movie_id INT REFERENCES movie(movie_id)
    """
    db.create_table("wishlist", columns)

def create_employee_table(db):
    columns = """
        employee_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        position VARCHAR(255) NOT NULL
    """
    db.create_table("employee", columns)

def create_sales_table(db):
    columns = """
        sales_id SERIAL PRIMARY KEY,
        month DATE,
        profit DECIMAL(10, 2)
    """
    db.create_table("sales", columns)

if __name__ == "__main__":
    db = Database("your_database_name", "your_username", "your_password", "your_host", "your_port")

    create_user_table(db)
    create_profile_table(db)
    create_billing_table(db)
    create_invoice_table(db)
    create_streaming_session_table(db)
    create_subscription_tiers_table(db)
    create_movie_table(db)
    create_genre_table(db)
    create_watchlist_table(db)
    create_interaction_table(db)
    create_wishlist_table(db)
    create_employee_table(db)
    create_sales_table(db)

    db.commit_and_close()

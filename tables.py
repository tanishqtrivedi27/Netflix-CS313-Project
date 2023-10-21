import psycopg2
from decouple import config
from database import Database

def create_account_table(db):
    columns = """
        account_id SERIAL PRIMARY KEY,
        password VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL UNIQUE
    """
    db.create_table("account", columns)

def create_profile_table(db):
    columns = """
        profile_id SERIAL PRIMARY KEY,
        account_id INT REFERENCES account(account_id) on delete cascade,
        profile_name VARCHAR(255) NOT NULL,
        profile_password VARCHAR(255) NOT NULL
    """
    db.create_table("profile", columns)

def create_billing_table(db):
    columns = """
        billing_id SERIAL PRIMARY KEY,
        account_id INT REFERENCES account(account_id) on delete cascade,
        payment_mode VARCHAR(255) NOT NULL,
        subscription_type INT REFERENCES subscription_tiers(tier_id),
        billing_date TIMESTAMP NOT NULL,
        expiration_date TIMESTAMP NOT NULL
    """
    db.create_table("billing", columns)

# def create_invoice_table(db):
#     columns = """
#         billing_id SERIAL PRIMARY KEY,
#         account_id INT REFERENCES account(account_id),
#         payment_mode VARCHAR(255) NOT NULL,
#         subscription_type INT REFERENCES subscription_tiers(tier_id),
#         billing_date TIMESTAMP NOT NULL,
#         expiration_date TIMESTAMP NOT NULL
#     """
#     db.create_table("invoice", columns)

def create_session_table(db):
    columns = """
        session_id SERIAL PRIMARY KEY,
        account_id INT REFERENCES account(account_id) on delete cascade,
        profile_id INT REFERENCES profile(profile_id) on delete cascade,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    """
    db.create_table("session", columns)

def create_subscription_tiers_table(db):
    columns = """
        tier_id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL CHECK(name in ('Mobile','Basic','Standard','Premium')),
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
        release_date DATE,
        actor_id INT REFERENCES actor(actor_id),
        director_id INT REFERENCES director(director_id)
    """
    db.create_table("movie", columns)

def create_actor_table(db):
    columns = """
        actor_id SERIAL PRIMARY KEY,
        actor_name VARCHAR(255)
    """
    db.create_table("actor", columns)
    
def create_director_table(db):
    columns = """
        director_id SERIAL PRIMARY KEY,
        director_name VARCHAR(255)
    """
    db.create_table("director", columns)
    
def create_genre_table(db):
    columns = """
        genre_id SERIAL PRIMARY KEY,
        genre_name VARCHAR(255) NOT NULL
    """
    db.create_table("genre", columns)

def create_watchlist_table(db):
    columns = """
        account_id INT REFERENCES account(account_id) on delete cascade,
        profile_id INT REFERENCES profile(profile_id) on delete cascade,
        movie_id INT REFERENCES movie(movie_id) on delete cascade,
        rating VARCHAR(255) CHECK (rating IN ('Not for me', 'I like this', 'Love this')),
        timestamp TIME,
        PRIMARY KEY (account_id, profile_id, movie_id)
    """
    db.create_table("watchlist", columns)

def create_wishlist_table(db):
    columns = """
        account_id INT REFERENCES account(account_id) on delete cascade,
        profile_id INT REFERENCES profile(profile_id) on delete cascade,
        movie_id INT REFERENCES movie(movie_id) on delete cascade,
        PRIMARY KEY (account_id, profile_id, movie_id)
    """
    db.create_table("wishlist", columns)

def create_revenue_table(db):
    columns = """
        month varchar(255) not null check(month in ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')),
        year int not null check(year>=2000 and year <=3000),
        revenue int not null
    """
    db.create_table("revenue", columns)
    
def create_net_revenue_table(db):
    columns = """
        month varchar(255) not null check(month in ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')),
        year int not null check(year>=2000 and year <=3000),
        revenue int not null
    """
    db.create_table("net_revenue", columns)
    
def create_movie_deals(db):
    columns = """
    deal_no SERIAL PRIMARY KEY,
    production_house_id int,
    movie_id int REFERENCES movie(movie_id) on delete cascade,
    price int    
    """
    db.create_table("movie_deals", columns)

def create_database():
    conn = psycopg2.connect(
            dbname="postgres",
            user=config('DB_USER'),
            password=config('DB_PASSWORD'),
            host=config('DB_HOST'),
            port=config('DB_PORT')
        )
    cur = conn.cursor()
    
    cur.execute("SELECT 'CREATE DATABASE netflix' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'netflix')")
    conn.commit()
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)

    create_account_table(db)
    create_profile_table(db)
    create_subscription_tiers_table(db)
    create_billing_table(db)
    create_session_table(db)
    create_actor_table(db)
    create_director_table(db)
    create_genre_table(db)
    create_movie_table(db)
    create_watchlist_table(db)
    create_wishlist_table(db)
    create_revenue_table(db)
    create_net_revenue_table(db)
    create_movie_deals(db)

    db.commit_and_close()

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

    def execute_query(self, query):
        self.cur.execute(query)
        
    def fetch_one(self):
        return self.cur.fetchone()

    def fetch_all(self):
        return self.cur.fetchall()
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()

    def commit_and_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

class UserQueries:
    def __init__(self, db):
        self.db = db

    def create_user(self, password, email):
        query = f'INSERT INTO user (password, email) VALUES ({password}, {email});'
        self.db.execute_query(query)
        return self.db.fetchone()

    def get_user_by_email_password(self, email, password):
        query = f'SELECT user_id FROM user WHERE email = {email} AND password = {password};'
        self.db.execute_query(query)
        return self.db.fetch_all()
    
    def update_user_password(self, email, old_password, new_password):
        if(self.get_user_by_email_password(email, old_password)):
            query = f'update user set password={new_password} WHERE email = {email} AND password = {old_password};'
            self.db.execute_query(query)
            print(" UPDATED SUCCESSFULLY")
        else:
            print("WRONG EMAIL OR OLD PASSWORD")
            
    
    def delete_user_by_email_password(self, email, password):
        query = f'delete FROM user WHERE email = {email} AND password = {password};'
        self.db.execute_query(query)
        print("DELETED SUCCESSFULLY!")
        return self.db.fetch_all()

class MovieQueries:
    def __init__(self, db):
        self.db = db

    def create_movie(self, title, genre_id, description, release_date, actor_id, director_id):
        query = f'INSERT INTO movie (title, genre_id, description, release_date, actor_id, director_id) VALUES ({title}, {genre_id}, {description}, {release_date}, {actor_id}, {director_id});'
        self.db.execute_query(query)
        print("MOVIE ADDED!")
        return self.db.fetchone()

    def get_movie_by_movieid(self, movie_id):
        query = f'SELECT * FROM movie WHERE movie_id = {movie_id};'
        self.db.execute_query(query)
        print(self.db.fetch_all())
    
    def delete_movie_by_movieid(self, movie_id):
        query = f'delete FROM movie WHERE movie_id = {movie_id};'
        self.db.execute_query(query)
        print("DELETED MOVIE!")

class User:
    def __init__(self, user_id):
        
        self.db_name = config('DB_NAME')
        self.db_user = config('DB_USER')
        self.db_password = config('DB_PASSWORD')
        self.db_host = config('DB_HOST')
        self.db_port = config('DB_PORT')
        
        self.db = Database(self.db_name, self.db_user, self.db_password, self.db_host, self.db_port)
        # self.user_queries = UserQueries(self.db)
        # self.movie_queries = MovieQueries(self.db)

        self.user_id = user_id
        self.profile_id = None
        
    def create_profile(self, profile_name, profile_password):
        query1 = f'SELECT count(*) from profile WHERE user_id = {self.user_id}';
        self.db.execute_query(query1)
        count_prof = self.db.fetch_one()[0]
        if(count_prof >= 6):
            print("Cannot add more profiles")
        else:        
            query = f'INSERT INTO profile (user_id, profile_name, profile_password) VALUES ({self.user_id},{profile_name}, {profile_password});'
            self.db.execute_query(query)
            print("Profile Created!")
        
    def login_profile(self, profile_name, profile_password):
        query = f'SELECT * FROM PROFILE WHERE profile_name={profile_name} and profile_password = {profile_password} AND user_id = {self.user_id};'
        self.db.execute_query(query)
        ret1 = self.db.fetch_one()
        if (ret1):
            self.profile_id = ret1[0]
            print("Succesfully logged in profile")
        else:
            print("No such profile name or password")
    
    def logout_profile(self):
        self.profile_id = None
        
    def add_movie_to_watchlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetchone()
        print(res1)
        time_stamp = "00:00:00"
        query = f'INSERT INTO watchlist values (self.user_id, self.profile_id, movie_id, NULL, {time_stamp});'
        self.db.execute_query(query)
        self.db.commit()
    
    def add_movie_to_wishlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetchone()
        print(res1)
        query = f'INSERT INTO wishlist values (self.user_id, self.profile_id, movie_id);'
        self.db.execute_query(query)
        self.db.commit()

    def update_movie_timestamp(self, movie_id, timestamp):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetchone()
        print(res1)
        query = f'UPDATE watchlist SET timestamp= {timestamp} WHERE (user_id, profile_id, MOVIE_id) = ({self.user_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()
    
    def delete_movie_from_wishlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetchone()
        print(res1)
        query = f'DELETE FROM wishlist WHERE (user_id, profile_id, MOVIE_id) = ({self.user_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()
        
    def update_user_password(self, old_password, new_password):
        query1 = f'SELECT * FROM USER WHERE user_id = {self.user_id};'
        self.db.execute_query(query1);
        old = self.db.fetch_all()[1]
        
        if (old_password == old):
            query = f'update USER SET password = {new_password} WHERE user_id = {self.user_id};'
            self.db.execute_query(query)
            print(" UPDATED SUCCESSFULLY")
        else:
            print("OLD PASSWORD doesn't match")
            
    
    def delete_user(self):
        query = f'delete FROM user WHERE USER_id={self.user_id};'
        self.db.execute_query(query)
        print("DELETED SUCCESSFULLY!")
        
    def logout(self):
        self.db.commit_and_close()

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
    

def signup(email, password, profile, profile_password):
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)
    
    query = f'SELECT * FROM USER WHERE email = {email};'
    db.execute_query(query)
    if(db.fetch_all()):
        print("ACCOUNT ALREADY EXISTS!")
    else:
        query1 = f'INSERT INTO USER (EMAIL, PASSWORD) VALUES({email}, {password});'
        db.execute_query(query1)
        print("ACCOUNT CREATED! YOU CAN LOGIN NOW")
    
    db.commit_and_close()    
    

def login(email, password):
    # Check if the user exists
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)


    query = f'SELECT * FROM USER WHERE EMAIL={email} and password = {password};'
    db.execute_query(query)
    ret1 = db.fetch_one()
    if(ret1):
        return User(ret1[0])
    else:
        print("Wrong password or email")
        
    db.commit_and_close()
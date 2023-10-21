from decouple import config
from tables import Database
import re
import numpy as np
from datetime import datetime
import redis


class UserQueries:
    def __init__(self, db):
        self.db = db

    def create_account(self, password, email):
        query = f'INSERT INTO account (password, email) VALUES ({password}, {email});'
        self.db.execute_query(query)
        return self.db.fetch_one()

    def get_account_by_email_password(self, email, password):
        query = f'SELECT account_id FROM account WHERE email = {email} AND password = {password};'
        self.db.execute_query(query)
        return self.db.fetch_all()
    
    def update_account_password(self, email, old_password, new_password):
        if(self.get_account_by_email_password(email, old_password)):
            query = f'update account set password={new_password} WHERE email = {email} AND password = {old_password};'
            self.db.execute_query(query)
            print(" UPDATED SUCCESSFULLY")
        else:
            print("WRONG EMAIL OR OLD PASSWORD")
            
    
    def delete_account_by_email_password(self, email, password):
        query = f'delete FROM account WHERE email = {email} AND password = {password};'
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
        return self.db.fetch_one()

    def get_movie_by_movieid(self, movie_id):
        query = f'SELECT * FROM movie WHERE movie_id = {movie_id};'
        self.db.execute_query(query)
        print(self.db.fetch_all())
    
    def delete_movie_by_movieid(self, movie_id):
        query = f'delete FROM movie WHERE movie_id = {movie_id};'
        self.db.execute_query(query)
        print("DELETED MOVIE!")

class Account:
    def __init__(self, account_id):
        
        self._db_name = config('DB_NAME')
        self._db_user= config('DB_USER')
        self._db_password = config('DB_PASSWORD')
        self._db_host = config('DB_HOST')
        self._db_port = config('DB_PORT')
        self.redisdb = RedisDB()
        
        
        self.db = Database(self._db_name, self._db_user, self._db_password, self._db_host, self._db_port)

        self.account_id = account_id
        self.profile_id = None

    def create_profile(self, profile_name, profile_password):
        query1 = f'SELECT count(*) from profile WHERE account_id = {self.account_id}';
        self.db.execute_query(query1)
        cnt = self.db.fetch_one()
        if (cnt is not None):
            count_prof = cnt[0]
            if(count_prof >= 6):
                print("Cannot add more profiles")
            else:        
                query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},{profile_name}, {profile_password});'
                self.db.execute_query(query)
                self.db.commit()
                print("Profile Created!")
        else: # first profile is getting created
            query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},{profile_name}, {profile_password});'
            self.db.execute_query(query)
            print("Profile Created!")
            self.db.commit()
            
    
    def login_profile(self, profile_name, profile_password):
        
        query1 = f'SELECT count(*) FROM SESSION where account_id={self.account_id} ;'
        self.db.execute_query(query1)
        # self.db.commit()
        if(self.db.fetch_one() is None or self.db.fetch_one()[0] < 4):
           
            query = f'SELECT * FROM PROFILE WHERE profile_name={profile_name} and profile_password = {profile_password} AND account_id = {self.account_id};'
            self.db.execute_query(query)
            profileID = self.db.fetch_one()
            if (profileID):
                self.profile_id = profileID[0]
                query2 = f'INSERT INTO session (account_id, profile_id, start_time) values (self.account_id, self.profile_id, datetime.now());'
                self.db.execute_query(query2)
                print("Succesfully logged in profile")
                self.db.commit()
                # query1 = f'INSERT INTO '
            else:
                print("No such profile name or password")
        else:
            print("Exceeded number of sessions")
    
    def logout_profile(self):
        query1 = f'DELETE from session where (account_id, profile_id) = ({self.account_id}, {self.profile_id})'
        self.db.execute_query(query1)
        self.db.commit()
        self.profile_id = None
        
    def add_movie_to_watchlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        cur_genre = res1[2]
        print(res1)
        time_stamp = "00:00:00"
        query = f'INSERT INTO watchlist values (self.account_id, self.profile_id, movie_id, NULL, {time_stamp});'
        self.db.execute_query(query)
        self.db.commit()
        
        query2 = f'SELECT title FROM MOVIE WHERE genre_id={cur_genre} LIMIT 5;'
        self.db.execute_query(query2)
        rec_movie = self.db.fetch_all()
        for i in rec_movie:
            self.redisdb.add_recommendation(self.account_id, self.profile_id, i)
            
        
        # 
    
    def add_movie_to_wishlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        print(res1)
        query = f'INSERT INTO wishlist values (self.account_id, self.profile_id, movie_id);'
        self.db.execute_query(query)
        self.db.commit()

    def update_movie_timestamp(self, movie_id, timestamp):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        print(res1)
        query = f'UPDATE watchlist SET timestamp= {timestamp} WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()
    
    def delete_movie_from_wishlist(self, movie_id):
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        print(res1)
        query = f'DELETE FROM wishlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()

    def update_account_password(self, old_password, new_password):
        query1 = f'SELECT * FROM USER WHERE account_id = {self.account_id};'
        self.db.execute_query(query1);
        old = self.db.fetch_all()[0][1]
        
        if (old_password == old):
            query = f'update USER SET password = {new_password} WHERE account_id = {self.account_id};'
            self.db.execute_query(query)
            print(" UPDATED SUCCESSFULLY")
        else:
            print("OLD PASSWORD doesn't match")
        self.db.commit()
    
    def payment_subscription(self,subscription_tier, payment_mode):
        subscription_tiers = ['Mobile','Basic','Standard','Premium']
        if(subscription_tier in subscription_tiers):
            query = f'SELECT * from subscription_tiers where name ={subscription_tier};'
            self.db.execute_query(query)
            self.db.commit()
            
            sub_id = self.db.fetch_one()[0]
            expiration_date = datetime.now()+datetime.relativedelta(months=1)
            
            query1 = f'INSERT INTO billing (account_id, payment_mode,subscription_type, billing_date, expiration_date) VALUES (self.account_id,{payment_mode},{sub_id},{datetime.now()},{expiration_date});'
            
            prob = np.random.randn()
            self.db.execute_query(query1)
            
            if(prob < 0.1):
                self.db.rollback()
                print("TRANSACTION FAILED")
            else:
                print("TRANSACTION SUCCESSFUL")
            self.db.commit()
        else:
            print("SUBSCRIPTION TIER IS NOT MATCHING")
            
    def get_user_recommendation(self):
        rec_movies = self.redisdb.get_recommendation(self.account_id,self.profile_id)
        if(len(rec_movies) == 0):
            query2 = f'SELECT title FROM MOVIE LIMIT 5;'
            self.db.execute_query(query2)
            rec_movie = self.db.fetch_all()
            return rec_movie
        else:
            return rec_movies
                  
    def delete_account_profile(self):    
        query = f'delete FROM profile WHERE USER_id={self.account_id} and profile_id = {self.profile_id};'
        self.db.execute_query(query)
        print("DELETED profile SUCCESSFULLY!")
        
        self.logout_profile()
        self.db.commit()
        
    def delete_account(self):
        query = f'DELETE FROM USER WHERE account_id = {self.account_id};'
        self.db.execute_query(query)
        self.db.commit()
        self.logout()
                
    def logout(self):
        self.db.commit_and_close()
        self.redisdb.close()

def signup(email, password):
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)
    query = f'SELECT * FROM USER WHERE email = {email};'
    
    if(is_valid_email(email)):
        try:
            db.execute_query(query)
            if(db.fetch_all()):
                print("ACCOUNT ALREADY EXISTS!")
            else:
                query1 = f'INSERT INTO USER (EMAIL, PASSWORD) VALUES({email}, {password});'
                db.execute_query(query1)
                print("ACCOUNT CREATED! YOU CAN LOGIN NOW")
                db.commit()
        except Exception as e:
            print("USER SIGN-UP FAILED")
            db.rollback()
    else:
        print("INVALID EMAIL")
        
        
    db.commit_and_close()    


def is_valid_email(email):
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_pattern, email)


def login(email, password) -> Account or None:
    # Check if the user exists
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)


    query = f'SELECT * FROM USER WHERE EMAIL={email} and password = {password};'
    try:
        db.execute_query(query)
        ret1 = db.fetch_one()
        if(ret1):
            return Account(ret1[0])
        else:
            print("Wrong password or email")
    except Exception as e:
        db.rollback()
        
    db.commit_and_close()
    return None

def logout(account: Account):
    account.logout()
    del account
    
class RedisDB:
    def __init__(self):
        self._dbhost = config('REDIS_HOST')
        self._dbport = config('REDIS_PORT')
        self._dbname = config('REDIS_DB')
        self.r = redis.Redis(host=self._dbhost, port=self._dbport, db=self._dbname)
        
    def add_recommendation(self,user_id,profile_id,movie):
        name = str(user_id) + "_"+ str(profile_id)
        self.r.rpush(name,movie)
        
    def get_recommendation(self,user_id, profile_id):
        name = str(user_id) + "_"+ str(profile_id)
        rec_list = self.r.lrange(name, 0,5)
        return rec_list
    
    

# Example usage:
if __name__ == "__main__":
    # Create a account
    signup("tanishq.trivedi27@gmail.com", "123456")
    # Log in
    account1 = login("tanishq.trivedi27@gmail.com", "123456")

    # Add a movie to the watchlist
    if (account1 is not None):
        account1.create_profile("tanishq", "1111")
        account1.login_profile("tanishq", "1111")
        account1.add_movie_to_watchlist(11)
        account1.update_movie_timestamp(11, "1:22:45")

        logout(account1)
fetch_all
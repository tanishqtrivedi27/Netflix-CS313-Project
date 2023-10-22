from decouple import config
from tables import Database
import re
import numpy as np
from datetime import datetime
import redis
from dateutil.relativedelta import relativedelta
import random


class UserQueries:
    def __init__(self, db):
        self.db = db

    def create_account(self, password, email):
        query = f'INSERT INTO account (password, email) VALUES (\'{password}\', \'{email}\');'
        self.db.execute_query(query)
        return self.db.fetch_one()

    def get_account_by_email_password(self, email, password):
        query = f'SELECT account_id FROM account WHERE email = \'{email}\' AND password = \'{password}\';'
        self.db.execute_query(query)
        return self.db.fetch_all()
    
    def update_account_password(self, email, old_password, new_password):
        if(self.get_account_by_email_password(email, old_password)):
            query = f'update account set password=\'{new_password}\' WHERE email = \'{email}\' AND password = \'{old_password}\';'
            self.db.execute_query(query)
            print(" UPDATED SUCCESSFULLY")
        else:
            print("WRONG EMAIL OR OLD PASSWORD")
            
    
    def delete_account_by_email_password(self, email, password):
        query = f'delete FROM account WHERE email = \'{email}\' AND password = \'{password}\';'
        self.db.execute_query(query)
        print("DELETED SUCCESSFULLY!")
        return self.db.fetch_all()

class MovieQueries:
    def __init__(self, db):
        self.db = db

    def create_movie(self, title, genre_id, description, release_date, actor_id, director_id, prod_id, price):
        query = f'INSERT INTO movie (title, genre_id, description, release_date, actor_id, director_id) VALUES (\'{title}\', {genre_id}, \'{description}\', \'{release_date}\', {actor_id}, {director_id}) RETURNING movie_id;'
        self.db.execute_query(query)
        mid = self.db.fetch_one()[0]
        
        # self.db.commit()
        
        # TRANSACTION 2
        query2 = f'INSERT INTO movie_deals (production_house_id,movie_id, price) VALUES ({prod_id},{mid},{price});'
        self.db.execute_query(query2)
        
        query4 = f'SELECT revenue FROM REVENUE WHERE MONTH = \'{datetime.now().strftime("%b")}\' and year ={datetime.now().strftime("%Y")};'
        self.db.execute_query(query4)
        res4 = self.db.fetch_one()
        res4 = 0 if (res4 is None) else res4[0]
            
        query5 = f'SELECT revenue FROM NET_REVENUE WHERE MONTH = \'{datetime.now().strftime("%b")}\' and year ={datetime.now().strftime("%Y")};'
        self.db.execute_query(query5)
        res5 = self.db.fetch_one()
        res5 = 0 if (res5 is None) else res5[0]
        
        if(res5 == 0):
            query6 = f'INSERT INTO NET_REVENUE VALUES(\'{datetime.now().strftime("%b")}\',{datetime.now().strftime("%Y")},{res4 -price});'
            
        else:
            query6 = f'UPDATE NET_REVENUE SET revenue = {res5 - price} WHERE MONTH = \'{datetime.now().strftime("%b")}\' and year ={datetime.now().strftime("%Y")};'
        
        self.db.execute_query(query6)
        
        prob = random.random()
        print("probablity of SOMETHING: ", prob)
        if (prob > 0.5):
            print("TRANSACTION FAILED")
            self.db.rollback()
        else:
            print("TRANSACTION SUCCESFUL")
            self.db.commit()
            print("MOVIE ADDED!")
            
        self.db.commit()
        return 

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

    def _check_profilelogin(self):
        if (self.profile_id is None):
            print("Login first")
            return True

        return False
        
    def create_profile(self, profile_name, profile_password):
        query1 = f'SELECT count(*) from profile WHERE account_id = {self.account_id};'
        self.db.execute_query(query1)
        cnt = self.db.fetch_one()
        
        query3 = f'SELECT count(*) from profile WHERE account_id = {self.account_id} and profile_name=\'{profile_name}\';'
        self.db.execute_query(query3)
        cnt2 = self.db.fetch_one()
        if(cnt2[0] !=0):
            print("PROFILE WITH SAME NAME EXISTS")
            return
            
        if (cnt is not None):
            if(cnt[0] >= 6):
                print("CANNOT ADD MORE PROFILES")
            else:        
                query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},\'{profile_name}\', \'{profile_password}\');'
                self.db.execute_query(query)
                self.db.commit()
                print(f'Profile Created for {profile_name}!')
        else: # first profile is getting created
            query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},\'{profile_name}\', \'{profile_password}\');'
            self.db.execute_query(query)
            print(f'Profile Created for {profile_name}!')
            self.db.commit()
            
    def login_profile(self, profile_name, profile_password):
        
        if(self.redisdb.get_num_devices(self.account_id)>=4):
            print("EXCEEDED NUMBER OF PERMITTED DEVICES")
            return
        
        if(self.profile_id):
            print("LOGOUT FROM PREVIOUS PROFILE")
            return
        
        query1 = f'SELECT count(*) FROM SESSION where account_id={self.account_id} ;'
        self.db.execute_query(query1)
        # self.db.commit()
        temp = self.db.fetch_one()
        if(temp is not None):
            # print(self.db.fetch_one())
            if(temp[0] < 4):
                query = f'SELECT * FROM PROFILE WHERE profile_name=\'{profile_name}\' and profile_password = \'{profile_password}\' AND account_id = {self.account_id};'
                self.db.execute_query(query)
                profileID = self.db.fetch_one()
                
                if (profileID):
                    
                    queryn = f'SELECT count(*) FROM SESSION where account_id={self.account_id} and profile_id = {profileID[0]};'
                    self.db.execute_query(queryn)
                    cnt_id = self.db.fetch_one()
                    
                    if(cnt_id[0] > 0 ):
                        print("PROFILE ALREADY LOGGED IN ")
                        return
                    
                    self.profile_id = profileID[0]
                    query2 = f'INSERT INTO session (account_id, profile_id, start_time) values ({self.account_id}, {self.profile_id}, \'{datetime.now()}\');'
                    self.db.execute_query(query2)
                    print("Succesfully logged in profile")
                    self.db.commit()
                    if(self.redisdb.get_num_devices(self.account_id)==0):
                        self.redisdb.set_num_devices(self.account_id)
                    else:
                        self.redisdb.incr_num_devices(self.account_id,1)
                    # query1 = f'INSERT INTO '
                else:
                    print("No such profile name or password")
            else:
                print("Exceeded number of sessions")
        else:
            query = f'SELECT * FROM PROFILE WHERE profile_name=\'{profile_name}\' and profile_password = \'{profile_password}\' AND account_id = {self.account_id};'
            self.db.execute_query(query)
            profileID = self.db.fetch_one()
            if (profileID):
                self.profile_id = profileID[0]
                query2 = f'INSERT INTO session (account_id, profile_id, start_time) values ({self.account_id}, {self.profile_id}, \'{datetime.now()}\');'
                self.db.execute_query(query2)
                print("Succesfully logged in profile")
                self.db.commit()
                if(self.redisdb.get_num_devices(self.account_id)==0):
                        self.redisdb.set_num_devices(self.account_id)
                else:
                    self.redisdb.incr_num_devices(self.account_id,1)
                # query1 = f'INSERT INTO '
            else:
                print("No such profile name or password") 
    
    def logout_profile(self):
        if(self._check_profilelogin()):
            return
        
        query1 = f'DELETE from session where (account_id, profile_id) = ({self.account_id}, {self.profile_id})'
        self.db.execute_query(query1)
        self.db.commit()
        self.profile_id = None
        print("SUCCesfully logged out profile")
        self.redisdb.incr_num_devices(self.account_id, -1)
        return
        
    def add_movie_to_watchlist(self, movie_id):
        if(self._check_profilelogin()):
            return
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        if(res1 is None):
            print("INVALID MOVIE ID")
            return
        self.db.commit()
        cur_genre = res1[2]
        time_stamp = "00:00:00"
        try:
            query = f'INSERT INTO watchlist values ({self.account_id}, {self.profile_id}, {movie_id}, NULL, \'{time_stamp}\');'
            self.db.execute_query(query)
            self.db.commit()
            print("MOVIE ADDED TO WATCHLIST")
        except Exception as e:
            print("MOVIE ALREADY PRESENT IN WATCHLIST")
            self.db.rollback()
            return
        # Movie updating recommendation
        query2 = f'SELECT title FROM MOVIE WHERE genre_id={cur_genre} LIMIT 5;'
        self.db.execute_query(query2)
        rec_movie = self.db.fetch_all()
        for i in rec_movie:
            self.redisdb.add_recommendation(self.account_id, self.profile_id, i[0])
            
    def add_movie_to_wishlist(self, movie_id):
        if(self._check_profilelogin()):
            return

        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        if(res1 is None):
            print("INVALID MOVIE ID")
            return
        self.db.commit()
        try:
            query = f'INSERT INTO wishlist values ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(query)
            self.db.commit()
            print("MOVIE ADDED TO WISHLIST")
        except Exception as e:
            print("MOVIE ALREADY PRESENT IN WISHLIST")
            self.db.rollback()
            return

    def update_movie_timestamp(self, movie_id, timestamp):
        if(self._check_profilelogin()):
            return
        
        query1 = f'SELECT * FROM watchlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        # print(res1)
        if(res1 is None):
            print("NO SUCH MOVIE IN WATCHLIST")
            return
        
        query = f'UPDATE watchlist SET timestamp= \'{timestamp}\' WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()
        print("MOVIE TIMESTAMP UPDATED")
    
    def delete_movie_from_wishlist(self, movie_id):
        if(self._check_profilelogin()):
            return
        
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        # data1 = (movie_id)
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        self.db.commit()
        if(res1 is None):
            print("INVALID MOVIE")
            return
        
        try:
            queryn = f'SELECT * FROM wishlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(queryn)
            resn = self.db.fetch_one()
            if(resn is None):
                print("MOVIE NOT PRESENT IN WISHLIST")
                return
            
            query = f'DELETE FROM wishlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(query)
            self.db.commit()
            print("MOVIE DELETED FROM WISHLIST")
        except Exception as e:
            print("MOVIE NOT PRESENT IN WISHLIST")
            self.db.rollback()
            return

    def update_account_password(self, old_password, new_password):
        query1 = f'SELECT * FROM account WHERE account_id = {self.account_id};'
        self.db.execute_query(query1)
        old = self.db.fetch_all()[0][1]
        
        if (old_password == old):
            query = f'update account SET password = \'{new_password}\' WHERE account_id = {self.account_id};'
            self.db.execute_query(query)
            print(" UPDATED ACCOUNT PASSWORD SUCCESSFULLY")
        else:
            print("OLD PASSWORD of ACCOUNT doesn't match")
            self.db.rollback()
        self.db.commit()
        
    def update_profile_password(self, old_password, new_password):
        if(self._check_profilelogin()):
            return
        
        query1 = f'SELECT * FROM profile WHERE account_id = {self.account_id} and profile_id ={self.profile_id};'
        self.db.execute_query(query1)
        old = self.db.fetch_all()[0][3]
        
        if (old_password == old):
            query = f'update profile SET profile_password = \'{new_password}\' WHERE account_id = {self.account_id} and profile_id = {self.profile_id};'
            self.db.execute_query(query)
            print(" UPDATED PROFILE PASSWORD SUCCESSFULLY")
            self.db.commit()
        else:
            print("OLD PROFILE PASSWORD doesn't match")
            self.db.rollback()
        self.db.commit()
    
    def payment_subscription(self,subscription_tier, payment_mode):
        subscription_tiers = ['Mobile','Basic','Standard','Premium']
        if(subscription_tier in subscription_tiers):
            query = f'SELECT * from subscription_tiers where name =\'{subscription_tier}\';'
            self.db.execute_query(query)
            self.db.commit()
            
            x=self.db.fetch_one()
            sub_id = x[0]
            expiration_date = datetime.now().date()+relativedelta(days=30)
            
            queryz = f'SELECT * FROM billing WHERE account_id={self.account_id} AND expiration_date > DATE(NOW());'
            self.db.execute_query(queryz)
            resz = self.db.fetch_one()
            
            if(resz is not None):
                print("SUBSCRIPTION ALREADY PURCHASED FOR CURRENT MONTH")
                return                
            
            query1 = f'INSERT INTO billing (account_id, payment_mode,subscription_type, billing_date, expiration_date) VALUES ({self.account_id},\'{payment_mode}\',{sub_id},\'{datetime.now().date()}\',\'{expiration_date}\');'
            self.db.execute_query(query1)
            
            
            query2 = f'SELECT revenue FROM REVENUE WHERE MONTH = \'{datetime.now().strftime("%b")}\' and year ={datetime.now().strftime("%Y")};'
            self.db.execute_query(query2)
            res2 = self.db.fetch_one()
            if(res2 is not None):
                # add revenue
                query4 =f'UPDATE REVENUE SET revenue = {res2[0]+x[2]} WHERE MONTH = \'{datetime.now().strftime("%b")}\' and year ={datetime.now().strftime("%Y")}'
                self.db.execute_query(query4)
                
            else:
                query3 = f'INSERT INTO REVENUE (MONTH, year, revenue) VALUES (\'{datetime.now().strftime("%b")}\', {datetime.now().strftime("%Y")}, {x[2]});'
                self.db.execute_query(query3)

            prob = random.random()
            print(str(prob)+" - BANK TRANSACTION PROB")
            if(prob > 0.9):
                self.db.rollback()
                print("TRANSACTION FAILED")
            else:
                print("TRANSACTION SUCCESSFUL")
                self.db.commit()
                
            self.db.commit()
        else:
            print("SUBSCRIPTION TIER IS NOT MATCHING")
            
    def get_user_recommendation(self):
        if(self._check_profilelogin()):
            return
        
        rec_movies = self.redisdb.get_recommendation(self.account_id,self.profile_id)
        if(len(rec_movies) == 0):
            query2 = f'SELECT title FROM MOVIE LIMIT 5;'
            self.db.execute_query(query2)
            rec_movie = self.db.fetch_all()
            rec_ls = [i[0] for i in rec_movie]
            return rec_ls
        else:
            return rec_movies
                  
    def delete_account_profile(self):
        if(self._check_profilelogin()):
            return
        
        pid = self.profile_id
        self.logout_profile()
        
        query = f'delete FROM profile WHERE account_id={self.account_id} and profile_id = {pid};'
        self.db.execute_query(query)
        print("DELETED profile SUCCESSFULLY!")
        
        
        self.db.commit()
        
    def delete_account(self):
        query = f'DELETE FROM account WHERE account_id = {self.account_id};'
        self.db.execute_query(query)
        self.db.commit()
        self.logout()
        print("ACCOUNT DELETED SUCCESSFULLY")
                 
    def logout(self):
        self.db.commit_and_close()
        self.redisdb.r.close()

def signup(email, password):
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)
    query = f'SELECT * FROM account WHERE email = \'{email}\';'
    # print(query)
    if(is_valid_email(email)):
        try:
            db.execute_query(query)
            if(db.fetch_all()):
                print("ACCOUNT ALREADY EXISTS!")
            else:
                query1 = f'INSERT INTO account (EMAIL, PASSWORD) VALUES(\'{email}\', \'{password}\');'
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


    query = f'SELECT * FROM account WHERE EMAIL=\'{email}\' and password = \'{password}\';'
    try:
        db.execute_query(query)
        ret1 = db.fetch_one()
        if(ret1):
            print("Successfully logged in account")
            return Account(ret1[0])
        else:
            print("Wrong password or email")
    except Exception as e:
        db.rollback()
        
    db.commit_and_close()
    return None

def logout(account: Account):
    print("Logout account")
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
        self.r.sadd(name,movie)
        
    def get_recommendation(self,user_id, profile_id):
        name = str(user_id) + "_"+ str(profile_id)
        rec_list = self.r.smembers(name)
        list_rec = [i.decode("utf-8") for i in list(rec_list)]
        return list_rec[-5:]
    
    def get_num_devices(self,user_id):
        if (self.r.hget('num_devices',user_id) is None):
            return 0
        else:
            return int((self.r.hget('num_devices',user_id).decode("utf-8"))) 
    
    def incr_num_devices(self,user_id,incr):
        if(self.get_num_devices(user_id)==0 and incr == -1):
            return
        self.r.hincrby('num_devices',user_id,incr)
        return
    
    def set_num_devices(self,user_id):
        self.r.hset('num_devices',user_id,1) 
        
if __name__ == "__main__":
    # Create a account
    
    
    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)
    
    mv = MovieQueries(db)
    
    mv.create_movie("sarrix",1,"ABC",datetime.today(),1,1, 1, 1000)
    mv.create_movie("Inception",1,"LOLOLOLOLOLO",datetime.today(),1,1, 1, 4000)
    # mv.create_movie("Batman",1,"afdsv",datetime.today(),1,1)
    # mv.create_movie("Joy",1,"xdbxre",datetime.today(),2,1)
    # mv.create_movie("Her",1,"vjnzsik",datetime.today(),1,1)
    
    db.commit_and_close()
    # Log in
    
    # signup("tanishq.trivedi27@gmail.com", "123456")
    # signup("vivekpillai@gmail.com", "12345")
    
    # account1 = login("tanishq.trivedi27@gmail.com", "123456")
    # account2 = login("vivekpillai@gmail.com", "12345")
    # account3 = login("tanishq.trivedi27@gmail.com", "123456")
    # account4 = login("tanishq.trivedi27@gmail.com", "123456")
    
    # Add a movie to the watchlist
    # if (account1 is not None):
        
        # account1.payment_subscription('Standard','UPI')
        
        # account1.create_profile("tanishq", "1111")
        # account1.login_profile("tanishq", "1111")
        
        
        # account2.payment_subscription('Premium','Cash')
        # account1.logout_profile()
        # account2.create_profile("pillai", "1111")
        # account2.login_profile("pillai", "1111")
        
        # # account2.delete_account_profile()
        
        # account3.create_profile("muskan", "1111")
        # account3.login_profile("muskan", "1111")
        
        # account4.create_profile("gauri", "1111")
        # account4.login_profile("gauri", "1111")
        # account4.logout_profile()
        
        # account5 = login("tanishq.trivedi27@gmail.com", "123456")
        
        # account5.create_profile("mokshita", "1111")
        # account5.login_profile("mokshita", "1111")
        
        # account6 = login("tanishq.trivedi27@gmail.com", "123456")
        
        # account6.create_profile("manasvi", "1111")
        # account6.login_profile("manasvi", "1111")
        # account3.login_profile("pillai","lol")
        
        # account1.update_movie_timestamp(2,"1:23:35")
        # account1.update_movie_timestamp(10,"1:23:35")
        
        # account1.update_account_password("Kushal","123456")
        
        
        # account1.add_movie_to_watchlist(5)
        # account1.add_movie_to_watchlist(2)
        # account2.add_movie_to_watchlist(13)
        # account2.add_movie_to_watchlist(3)
        # print(account1.get_user_recommendation())
        # print(account2.get_user_recommendation())
        # account2.update_profile_password("lol","lol")
        
        # account1.add_movie_to_wishlist(4)
        # account1.add_movie_to_wishlist(3)
        # account1.add_movie_to_wishlist(4)
        # account1.delete_movie_from_wishlist(4)
        # account1.delete_movie_from_wishlist(5)

        # account1.logout_profile()    
              

        
        
        # account2.logout_profile()
        
        # account3.logout_profile()    
        
        
        
        # account5.logout_profile() 
        # # account6.logout_profile() 
        # # account1.delete_account()
        # account1.create_profile("avni", "6969")
        # account1.login_profile("avni", "6969")
        # logout(account1)
        # logout(account2)
        
        # logout(account3)
        
        # logout(account4)
        # logout(account5)
        # logout(account6)
        

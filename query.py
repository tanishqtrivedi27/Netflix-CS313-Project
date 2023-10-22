from decouple import config
from database import Database
from database import RedisDB
import re
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random

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

        # query to find whether user has an active subscription plan

        self._active_sub = False

        query = f'SELECT * from billing WHERE account_id = {self.account_id} AND expiration_date > DATE(NOW());'
        self.db.execute_query(query)
        resz = self.db.fetch_one()
        if (resz is not None):
            self._active_sub = True

        if (not self._active_sub):
            print("Select a subscription and PAY!!!")

        # query to find max num of devices allowed on subscription plan
        self._get_devices = 0
        if (self._active_sub):
            
            tier_id = resz[3]
            query2 = f'SELECT * from subscription_tiers WHERE tier_id = {tier_id};'
            self.db.execute_query(query2)
            self._get_devices = self.db.fetch_one()[3]

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
            return {'err': 0, 'msg': "PROFILE WITH SAME NAME EXISTS"}
            
        if (cnt is not None):
            if(cnt[0] >= 6):
                print("CANNOT ADD MORE PROFILES")
                return {'err': 0,'msg': "CANNOT ADD MORE PROFILES"}
            else:        
                query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},\'{profile_name}\', \'{profile_password}\');'
                self.db.execute_query(query)
                self.db.commit()
                print(f'Profile Created for {profile_name}!')
                return {'err': 1,'msg': f'Profile Created for {profile_name}! YOU CAN LOGIN NOW'}
        else: # first profile is getting created
            query = f'INSERT INTO profile (account_id, profile_name, profile_password) VALUES ({self.account_id},\'{profile_name}\', \'{profile_password}\');'
            self.db.execute_query(query)
            print(f'Profile Created for {profile_name}!')
            self.db.commit()
            return {'err': 1,'msg': f'Profile Created for {profile_name}! YOU CAN LOGIN NOW'}
          
    def login_profile(self, profile_name, profile_password):
        
        query = f'SELECT * from billing WHERE account_id = {self.account_id} AND expiration_date > DATE(NOW());'
        self.db.execute_query(query)
        resz = self.db.fetch_one()
        if (resz is not None):
            self._active_sub = True

        if (self._active_sub):
            
            tier_id = resz[3]
            query2 = f'SELECT * from subscription_tiers WHERE tier_id = {tier_id};'
            self.db.execute_query(query2)
            self._get_devices = self.db.fetch_one()[3]
        
        if (not self._active_sub):
            return {'err': 0, 'msg': "Buy a subscription plan"}
        
        if(self.redisdb.get_num_devices(self.account_id) >= self._get_devices):
            return {'err': 0, 'msg': "Exceeded number of devices"}
        
        if(self.profile_id):
            return {'err': 0, 'msg': "Logout from previous profile"}
        
        query1 = f'SELECT count(*) FROM SESSION where account_id={self.account_id} ;'
        self.db.execute_query(query1)
        # self.db.commit()
        temp = self.db.fetch_one()
        if(temp is not None):
            # print(self.db.fetch_one())
            if(temp[0] < self._get_devices):
                query = f'SELECT * FROM PROFILE WHERE profile_name=\'{profile_name}\' and profile_password = \'{profile_password}\' AND account_id = {self.account_id};'
                self.db.execute_query(query)
                profileID = self.db.fetch_one()
                
                if (profileID):
                    
                    queryn = f'SELECT count(*) FROM SESSION where account_id={self.account_id} and profile_id = {profileID[0]};'
                    self.db.execute_query(queryn)
                    cnt_id = self.db.fetch_one()
                    
                    if(cnt_id[0] > 0 ):
                        return {'err': 0, 'msg': "Profile already logged in"}
                    
                    self.profile_id = profileID[0]
                    query2 = f'INSERT INTO session (account_id, profile_id, start_time) values ({self.account_id}, {self.profile_id}, \'{datetime.now()}\');'
                    self.db.execute_query(query2)
                    
                    self.db.commit()
                    if(self.redisdb.get_num_devices(self.account_id)==0):
                        self.redisdb.set_num_devices(self.account_id)
                    else:
                        self.redisdb.incr_num_devices(self.account_id,1)
                    
                    return {'err': 1, 'msg': 'Succesfully logged in profile'}
                else:
                    return {'err': 0, 'msg': "Wrong profile name or password"}
            else:
                return {'err': 0, 'msg': "Exceeded number of devices"}
        else:
            query = f'SELECT * FROM PROFILE WHERE profile_name=\'{profile_name}\' and profile_password = \'{profile_password}\' AND account_id = {self.account_id};'
            self.db.execute_query(query)
            profileID = self.db.fetch_one()
            if (profileID):
                self.profile_id = profileID[0]
                query2 = f'INSERT INTO session (account_id, profile_id, start_time) values ({self.account_id}, {self.profile_id}, \'{datetime.now()}\');'
                self.db.execute_query(query2)
                self.db.commit()
                if(self.redisdb.get_num_devices(self.account_id)==0):
                        self.redisdb.set_num_devices(self.account_id)
                else:
                    self.redisdb.incr_num_devices(self.account_id,1)
                    
                return {'err': 1, 'msg': 'Succesfully logged in profile'}
            
            else:
                return {'err': 0, 'msg': "Wrong profile name or password"}
    
    def logout_profile(self):
        if(self._check_profilelogin()):
            return
        
        query1 = f'DELETE from session where (account_id, profile_id) = ({self.account_id}, {self.profile_id})'
        self.db.execute_query(query1)
        self.db.commit()
        self.profile_id = None
        print("Succesfully logged out profile")
        self.redisdb.incr_num_devices(self.account_id, -1)
        return {'err': 1, 'msg': 'Succesfully logged out profile'}
        
    def add_movie_to_watchlist(self, movie_id):
        if(self._check_profilelogin()):
            return
        
        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        if(res1 is None):
            print("INVALID MOVIE ID")
            return {'err': 0, 'msg': "INVALID MOVIE ID"}
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
            return {'err': 0, 'msg': "MOVIE ALREADY PRESENT IN WATCHLIST"}
        # Movie updating recommendation
        query2 = f'SELECT title FROM MOVIE WHERE genre_id={cur_genre} LIMIT 5;'
        self.db.execute_query(query2)
        rec_movie = self.db.fetch_all()
        for i in rec_movie:
            self.redisdb.add_recommendation(self.account_id, self.profile_id, i[0])
        return {'err': 1, 'msg': 'MOVIE ADDED TO WATCHLIST'}
            
    def add_movie_to_wishlist(self, movie_id):
        if(self._check_profilelogin()):
            return

        query1 = f'SELECT * FROM MOVIE WHERE MOVIE_ID={movie_id};'
        self.db.execute_query(query1)
        res1 = self.db.fetch_one()
        if(res1 is None):
            print("INVALID MOVIE ID")
            return {'err': 0, 'msg': "INVALID MOVIE ID"}
        self.db.commit()
        try:
            query = f'INSERT INTO wishlist values ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(query)
            self.db.commit()
            print("MOVIE ADDED TO WISHLIST")
        except Exception as e:
            print("MOVIE ALREADY PRESENT IN WISHLIST")
            self.db.rollback()
            return {'err': 0, 'msg': "MOVIE ALREADY PRESENT IN WISHLIST"}
        return {'err': 1, 'msg': 'MOVIE ADDED TO WISHLIST'}

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
            return  {'err': 0, 'msg': "NO SUCH MOVIE IN WATCHLIST"}
        
        query = f'UPDATE watchlist SET timestamp= \'{timestamp}\' WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
        self.db.execute_query(query)
        self.db.commit()
        print("MOVIE TIMESTAMP UPDATED")
        return {'err': 1, 'msg': "MOVIE TIMESTAMP UPDATED"}
    
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
            return {'err': 0, 'msg': 'INVALID MOVIE'}
        
        try:
            queryn = f'SELECT * FROM wishlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(queryn)
            resn = self.db.fetch_one()
            if(resn is None):
                print("MOVIE NOT PRESENT IN WISHLIST")
                return {'err': 0, 'msg': 'MOVIE NOT PRESENT IN WISHLIST'}
            
            query = f'DELETE FROM wishlist WHERE (account_id, profile_id, MOVIE_id) = ({self.account_id}, {self.profile_id}, {movie_id});'
            self.db.execute_query(query)
            self.db.commit()
            print("MOVIE DELETED FROM WISHLIST")
        except Exception as e:
            print("MOVIE NOT PRESENT IN WISHLIST")
            self.db.rollback()
            return {'err': 0, 'msg': 'MOVIE NOT PRESENT IN WISHLIST'}
        return {'err': 1, 'msg': 'MOVIE DELETED FROM WISHLIST'}

    def update_account_password(self, old_password, new_password):
        query1 = f'SELECT * FROM account WHERE account_id = {self.account_id};'
        self.db.execute_query(query1)
        old = self.db.fetch_all()[0][1]
        
        if (old_password == old):
            query = f'update account SET password = \'{new_password}\' WHERE account_id = {self.account_id};'
            self.db.execute_query(query)
            print("UPDATED ACCOUNT PASSWORD SUCCESSFULLY")
            self.db.commit()
            return {'err':1,'msg':'UPDATED ACCOUNT PASSWORD SUCCESSFULLY'}
        else:
            print("OLD PASSWORD of ACCOUNT doesn't match")
            self.db.rollback()
            return {'err':0,'msg':'OLD PASSWORD of ACCOUNT doesn\'t match'}
        # self.db.commit()
        
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
            return {'err':1,'msg':'UPDATED PROFILE PASSWORD SUCCESSFULLY'}
        else:
            print("OLD PROFILE PASSWORD doesn't match")
            self.db.rollback()
            return {'err':0,'msg':'OLD PASSWORD of profile doesn\'t match'}
        # self.db.commit()
    
    def payment_subscription(self,subscription_tier, payment_mode):
        subscription_tiers = ['Mobile','Basic','Standard','Premium']
        if(subscription_tier in subscription_tiers):
            query = f'SELECT * from subscription_tiers where name =\'{subscription_tier}\';'
            self.db.execute_query(query)
            self.db.commit()
            
            x=self.db.fetch_one()
            num_dev = x[3]
            sub_id = x[0]
            expiration_date = datetime.now().date()+relativedelta(days=30)
            
            queryz = f'SELECT * FROM billing WHERE account_id={self.account_id} AND expiration_date > DATE(NOW());'
            self.db.execute_query(queryz)
            resz = self.db.fetch_one()
            
            if(resz is not None):
                print("SUBSCRIPTION ALREADY PURCHASED FOR CURRENT MONTH")
                return {'err':0, 'msg':'SUBSCRIPTION ALREADY PURCHASED FOR CURRENT MONTH'}              
            
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
                return {'err':0, 'msg':'TRANSACTION FAILED! TRY AGAIN FOR PAYMENT'}
            else:
                self._active_sub = True
                self._get_devices = num_dev
                print("TRANSACTION SUCCESSFUL")
                self.db.commit()
                return {'err':1, 'msg':'TRANSACTION SUCCESSFUL! YOU CAN LOGIN TO A PROFILE'}

        else:
            print("SUBSCRIPTION TIER IS NOT MATCHING")
            return {'err':0, 'msg':'SUBSCRIPTION TIER IS NOT MATCHING'}
            
    def get_user_recommendation(self):
        if(self._check_profilelogin()):
            return
        
        rec_movies = self.redisdb.get_recommendation(self.account_id,self.profile_id)
        if(len(rec_movies) == 0):
            query2 = f'SELECT title FROM MOVIE LIMIT 5;'
            self.db.execute_query(query2)
            rec_movie = self.db.fetch_all()
            rec_ls = [i[0] for i in rec_movie]
            rec_ls_1 = ', '.join(rec_ls)
            return {'err':1,'msg':rec_ls_1}
        else:
            rec_movie_1 = ', '.join(rec_movies)
            return {'err':1,'msg':rec_movie_1}
               
    def resume_movie(self,movie_id):
        if(self._check_profilelogin()):
            return
        query = f'SELECT * FROM WATCHLIST WHERE account_id ={self.account_id} and profile_id ={self.profile_id} and movie_id = {movie_id};'
        self.db.execute_query(query)
        res = self.db.fetch_one()
        if(res is not None):
            print("MOVIE RESUMED FROM TIMESTAMP", res[4])
            self.db.commit()
            return {'err':1, 'msg':f'MOVIE RESUMED FROM TIMESTAMP {res[4]}'}
        else:
            self.db.rollback()
            print("YOU HAVE NOT WATCHED THE MOVIE YET")
            return {'err':0, 'msg':f'YOU HAVE NOT WATCHED THE MOVIE YET'}
        self.db.commit()
        
    def rate_movie(self,movie_id,rating):
        poss_rat = ['Not for me', 'I like this', 'Love this']
        if(rating not in poss_rat):
            print("INVALID RATING")
            return {'err': 0, 'msg': "INVALID RATING"}
        if(self._check_profilelogin()):
            return {'err': 0, 'msg': "LOGIN FIRST"}
        query = f'SELECT * FROM WATCHLIST WHERE account_id ={self.account_id} and profile_id ={self.profile_id} and movie_id = {movie_id};'
        self.db.execute_query(query)
        res = self.db.fetch_one()
        if(res is not None):
            query1 = f'UPDATE watchlist SET rating = \'{rating}\' WHERE account_id ={self.account_id} and profile_id ={self.profile_id} and movie_id = {movie_id};'
            self.db.execute_query(query1)
            print("RATED SUCCESSFULLY")
            self.db.commit()
            return {'err': 1,'msg': "RATED SUCCESSFULLY"}
        else:
            self.db.rollback()
            print("YOU HAVE NOT WATCHED THE MOVIE YET")
            return {'err': 0,'msg': "YOU HAVE NOT WATCHED THE MOVIE YET"}
        # self.db.commit()
                  
    def delete_account_profile(self):
        if(self._check_profilelogin()):
            return
        
        pid = self.profile_id
        self.logout_profile()
        
        query = f'delete FROM profile WHERE account_id={self.account_id} and profile_id = {pid};'
        self.db.execute_query(query)
        print("DELETED profile SUCCESSFULLY!")
        self.db.commit()
        return {'err':1, 'msg':f'DELETED profile SUCCESSFULLY!'}
        
    def delete_account(self):
        query = f'DELETE FROM account WHERE account_id = {self.account_id};'
        self.db.execute_query(query)
        self.db.commit()
        self.logout()
        print("ACCOUNT DELETED SUCCESSFULLY")
        return {'err':1, 'msg':f'DELETED ACCOUNT SUCCESSFULLY!'}
                 
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
                return {'msg': 'Account ALREADY EXISTS'}
            else:
                query1 = f'INSERT INTO account (EMAIL, PASSWORD) VALUES(\'{email}\', \'{password}\');'
                db.execute_query(query1)
                print("ACCOUNT CREATED! YOU CAN LOGIN NOW")
                db.commit()
                return {'msg': 'Account successfully created'}
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
    # del account
        
if __name__ == "__main__":

    signup("tanishq.trivedi27@gmail.com", "123456")
    signup("vivekpillai@gmail.com", "12345")
    
    account1 = login("tanishq.trivedi27@gmail.com", "123456")
    account2 = login("vivekpillai@gmail.com", "12345")
    account3 = login("vivekpillai@gmail.com", "12345")
    account4 = login("vivekpillai@gmail.com", "12345")
    
    # Add a movie to the watchlist
    if (account1 is not None):
        
        # account1.payment_subscription('Standard','UPI')
        
        account1.create_profile("tanishq", "1111")
        account1.login_profile("tanishq", "1111")

        account2.payment_subscription('Standard','Cash')
        # account1.logout_profile()
        account2.create_profile("pillai", "1111")
        account2.login_profile("pillai", "1111")
        
        # # account2.delete_account_profile()
        
        account3.create_profile("muskan", "1111")
        account3.login_profile("muskan", "1111")
        
        account4.create_profile("gauri", "1111")
        account4.login_profile("gauri", "1111")
        
        # account5 = login("tanishq.trivedi27@gmail.com", "123456")
        
        # account5.create_profile("mokshita", "1111")
        # account5.login_profile("mokshita", "1111")
        
        # account6 = login("tanishq.trivedi27@gmail.com", "123456")
        
        # account6.create_profile("manasvi", "1111")
        # account6.login_profile("manasvi", "1111")
        # account3.login_profile("pillai","lol")
        
        account1.update_movie_timestamp(2,"1:23:35")
        account1.update_movie_timestamp(10,"1:23:35")
        
        # account1.update_account_password("Kushal","123456")
        
        
        account1.add_movie_to_watchlist(19)
        account1.add_movie_to_watchlist(20)
        
        account1.update_movie_timestamp(19,"1:30:56")
        
        account1.resume_movie(25)
        account1.resume_movie(19)
        
        account1.rate_movie(20,"I like this")
        account1.rate_movie(29,"Not for me")
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

        account1.logout_profile()    
              

        
        
        account2.logout_profile()
        
        account3.logout_profile()    
        
        
        
        account4.logout_profile() 
        # # account6.logout_profile() 
        # # account1.delete_account()
        # account1.create_profile("avni", "6969")
        # account1.login_profile("avni", "6969")
        logout(account1)
        logout(account2)
        
        logout(account3)
        
        logout(account4)
        # logout(account5)
        # logout(account6)
        
from decouple import config
from database import Database
from database import RedisDB
from datetime import datetime
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
        
if __name__ == "__main__":

    db_name = config('DB_NAME')
    db_user = config('DB_USER')
    db_password = config('DB_PASSWORD')
    db_host = config('DB_HOST')
    db_port = config('DB_PORT')

    db = Database(db_name, db_user, db_password, db_host, db_port)
    
    mv = MovieQueries(db)
    
    mv.create_movie("Dune", 1, "description maybe", datetime.today(), 1, 1, 1, 1000)
    mv.create_movie("Radhe",1 ,"description maybe not", datetime.today(), 1, 1, 1, 4000)
    
    db.commit_and_close()
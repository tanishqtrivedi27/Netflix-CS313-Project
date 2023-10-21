from decouple import config
import redis
import psycopg2

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

    def create_table(self, table_name, columns):
        create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
        self.cur.execute(create_query)

    def commit_and_close(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()
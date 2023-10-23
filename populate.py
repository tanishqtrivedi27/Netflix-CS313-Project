import pandas as pd
import psycopg2
import random
from tqdm import tqdm
import codecs
from decouple import config
import ast

conn = psycopg2.connect(host=config('DB_HOST'),
                        dbname=config('DB_NAME'), 
                        user = config('DB_USER'), 
                        password=config('DB_PASSWORD'), 
                        port = config('DB_PORT'))
cur = conn.cursor()

data_mov = pd.read_csv('./data/movies.csv')
data_mov.head()

data_gen = pd.read_csv("./data/genre.csv")
data_gen.head()

def add_movie(mov_id , title , gen , des , date , act , dir):
    sql = f"INSERT INTO movie(movie_id , title , genre_id , description , release_date , actor_id , director_id) VALUES({mov_id} , '{title}' , '{gen}' , '{des}' , TO_DATE('{date}','DD-MM-YYYY') , {act} , {dir});"
    # print(sql)
    try :
        cur.execute(sql)
        conn.commit()
    except psycopg2.DatabaseError as e :
        print(e)
        conn.rollback()
                    

def add_genre(gen_id , gen_name):
    sql = f"INSERT INTO genre(genre_id , genre_name) VALUES ({gen_id},'{gen_name}');"
    try :
        cur.execute(sql)
        print(f'added {gen_id} , {gen_name}')
        conn.commit()
    except psycopg2.DatabaseError as e :
        print(e)
        conn.rollback()

def add_actor(act_id , act_name):
    sql = f"INSERT INTO actor(actor_id , actor_name) VALUES ({act_id},'{act_name}');"
    # print(sql)
    try :
        cur.execute(sql)
        conn.commit()
    except psycopg2.DatabaseError as e :
        print(e)
        conn.rollback()


def add_director(dir_id , dir_name):
    sql = f"INSERT INTO director(director_id , director_name) VALUES ({dir_id},'{dir_name}');"
    # print(sql)
    try :
        cur.execute(sql)
        conn.commit()
    except psycopg2.DatabaseError as e :
        print(e)
        conn.rollback()

def utf8_to_win1252(utf8_string):
    try:
        # Encode the UTF-8 string to WIN1252 and decode it to get a string
        win1252_bytes = codecs.encode(utf8_string, 'windows-1252', errors='ignore')
        win1252_string = win1252_bytes.decode('windows-1252')
        return win1252_string
    except Exception as e:
        # Handle the exception or return an error message
        return f"Error: {str(e)}"

for ind in data_gen.index:
    add_genre(int(data_gen['genre_id'][ind]) , str(data_gen['genre_name'][ind]))

data_act = pd.read_csv('./data/actors.csv')

for ind in tqdm(data_act.index):
    win_1252_string = codecs.encode(data_act['actor_name'][ind] , 'windows-1252' , errors='ignore').decode('windows-1252')
    add_actor(int(data_act['actor_id'][ind]) , str(win_1252_string).replace("'" , ""))

data_dir = pd.read_csv('./data/director.csv')

for ind in tqdm(data_dir.index):
    win_1252_string = codecs.encode(data_dir['director_name'][ind] , 'windows-1252' , errors='ignore').decode('windows-1252')
    add_director(int(data_dir['director_id'][ind]) , str(win_1252_string).replace("'" , ""))
    

count = 0
for ind in tqdm(data_mov.index):
        mov_id = int(data_mov['movie_id'][ind])

        title = str(data_mov['original_title'][ind]).replace("'" , "")
        # title = title.split(" ")[0]
        title = utf8_to_win1252(title)

        gen = random.randint(0, 19)

        des = str(data_mov['overview'][ind]).replace("'" , "")
        des = utf8_to_win1252(des)

        date = data_mov['release_date'][ind]
        if (date == 'na'):
            continue
       
        act = random.randint(1, 50000)

        dir = random.randint(1, 2570)

        add_movie(mov_id , title , gen , des , date , act , dir)
    

        

print(count)

cur.close()
conn.close()
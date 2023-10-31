from flask import Flask, request, jsonify, session, render_template, redirect, url_for
import time
from flask_session import Session
from query import Account,login,logout,signup
import psycopg2 
from decouple import config

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
app.secret_key = 'your_secret_key'
Session(app)

i=1
accounts = {}

@app.route('/',methods=['GET', 'POST'])
def home():
    msg = request.args.get('msg', default='', type=str)  # Retrieve the 'msg' argument if provided

    return render_template("login.html", msg=msg)

@app.route('/signup',methods=['POST','GET'])
def sign_up():
    if(request.method == 'POST'):
        email = request.form['email']
        password = request.form['password']
        x = signup(email,password)
        return redirect(url_for('home', msg=x['msg']))      
    return render_template('signup.html')
        

@app.route('/account_home',  methods=['GET', 'POST'])
def account_home():
    global i
    
    if(request.method == 'POST'):
        # LOGIN ACCOUNT
        if ('password' in request.form and 'email' in request.form):
            email = request.form['email']
            password = request.form['password']
            session['email'] = email
            session['password'] = password
            
            acc_name = f'account_{i}'
            temp_account = login(email,password)
            if(temp_account is None):
                mssg =  "Wrong Email or Wrong Password"
                return redirect(url_for('home', msg=mssg))   
            
            accounts[acc_name] = temp_account
            i+=1
            session['gauri'] = acc_name
        
            return render_template('account_home.html')

        # CREATE PROFILE
        if('op' in request.form and 'name' in request.form and 'profile_password' in request.form):
            name = request.form['name']
            profile_password = request.form['profile_password']

            x = accounts[session['gauri']].create_profile(name, profile_password)
            return render_template('account_home.html', msg=x['msg']) 

        # PAYMENT 
        elif ('Subscription' in request.form and 'payment_mode' in request.form):
            subscription = request.form['Subscription']
            payment_mode = request.form['payment_mode']
            x = accounts[session['gauri']].payment_subscription(subscription, payment_mode)
            return render_template('account_home.html', msg=x['msg'])
        
        # UPDATE PASSWORD
        elif ('old_pass' in request.form and 'new_pass' in request.form):
            old_pass = request.form['old_pass']
            new_pass = request.form['new_pass']
            x = accounts[session['gauri']].update_account_password(old_pass, new_pass)
            return render_template('account_home.html', msg=x['msg'])

        elif('op' in request.form):
            op = int(request.form['op'])
            print(op)
            print("IN LOGOUT OR DELETE")
            # DELETE ACCOUNT
            if (op == 5):
                x = accounts[session['gauri']].delete_account()
                del accounts[session['gauri']]
                del session['gauri']
                return render_template('login.html', msg=x['msg'])

            # logout ACCOUNT
            elif (op == 6):
                logout(accounts[session['gauri']])
                del accounts[session['gauri']]
                del session['gauri']
                return render_template('login.html', msg='LOGGED OUT OF ACCOUNT')
        else:
            return "something is wrong"
        
    # default, hope it never reaches here
    return render_template('account_home.html')

@app.route('/profile_home',  methods=['GET', 'POST'])
def profile_home():
    global i
    
    if request.method == 'POST':
        
        if('wish' in request.form):
            x = accounts[session['gauri']].show_wishlist()
            return render_template('profile_home.html', msg=x['msg'])
        
        if('watch' in request.form):
            x = accounts[session['gauri']].show_watchlist()
            return render_template('profile_home.html', msg=x['msg'])
        
        if ('title' in request.form):
            title = request.form['title']
            conn = psycopg2.connect(
                dbname=config('DB_NAME'),
                user=config('DB_USER'),
                password=config('DB_PASSWORD'),
                host=config('DB_HOST'),
                port=config('DB_PORT')
            )
            cur = conn.cursor()
            query = f'SELECT * FROM movie WHERE title = \'{title}\';'
            cur.execute(query)
            res = cur.fetchone()
            if(res is None):
                cur.close()
                conn.close()
                return render_template('profile_home.html', msg="No such Movie found")
            else:
                cur.close()
                conn.close()
                return render_template('profile_home.html', msg=f'Movie Id of Given Movie: {res[0]}')
            
            
        # LOGIN PROFILE
        if ('name' in request.form and 'profile_password' in request.form):
            name = request.form['name']
            profile_password = request.form['profile_password']
            session['name'] = name
            session['profile_password'] = profile_password

            x = accounts[session['gauri']].login_profile(name, profile_password)
            
            if (x['err'] == 0):
                return render_template('account_home.html', msg=x['msg'])
            else:
                return render_template('profile_home.html', msg=x['msg'])

        if('mid' in request.form and 'rating' in request.form):
            mid = request.form['mid']
            rating = request.form['rating']
            y = accounts[session['gauri']].rate_movie(mid, rating)
            return render_template('profile_home.html',msg=y['msg'])
            
        elif('mid' in request.form and 'timestamp' in request.form):
            mid = request.form['mid']
            timestamp = request.form['timestamp']
            y = accounts[session['gauri']].update_movie_timestamp(mid, timestamp)
            return render_template('profile_home.html',msg=y['msg'])
        
        elif('new_pass' in request.form and 'old_pass' in request.form):
            new_pass = request.form['new_pass']
            old_pass = request.form['old_pass']
            y = accounts[session['gauri']].update_account_password(old_pass, new_pass)  
            return render_template('profile_home.html',msg=y['msg'])
        
        elif('mid' in request.form and 'opno' in request.form):
            op = int(request.form['opno'])
            mid = request.form['mid']
            if(op == 1):
                y = accounts[session['gauri']].add_movie_to_watchlist(mid)
                return render_template('profile_home.html',msg=y['msg'])
            elif(op == 2):
                y = accounts[session['gauri']].add_movie_to_wishlist(mid)
                return render_template('profile_home.html',msg=y['msg'])
            elif(op == 4):
                y = accounts[session['gauri']].delete_movie_from_wishlist(mid)
                return render_template('profile_home.html',msg=y['msg'])
            elif(op == 7):
                y = accounts[session['gauri']].resume_movie(mid)
                return render_template('profile_home.html',msg=y['msg'])
        
        elif('opno' in request.form):
            op = int(request.form['opno'])
            if(op == 10):
                y = accounts[session['gauri']].logout_profile()
                return render_template('account_home.html',msg=y['msg'])
            elif(op==9):
                y = accounts[session['gauri']].delete_account_profile()
                return render_template('account_home.html',msg=y['msg'])
        
        else:
            y=accounts[session['gauri']].get_user_recommendation()
            return render_template('profile_home.html',msg=y['msg'])
            
    return render_template('profile_home.html')


# class Account:
#     def __init__(self, STE1, SR2):
#         self.email = STE1
#         pass

#     def __str__(self):
#         return "XYZ"

#     def pp(self):
#         # print(self.email)
#         return self.email

if __name__ == '__main__':
    app.run(debug=True)

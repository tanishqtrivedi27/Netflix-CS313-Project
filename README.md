# Netflix-CS313-Project
CS313 Project  

Create .env file similar to .env.sample

```
pip install -r requirements.txt
```

To do:
1. create user class and related methods for 

                a method for login based on email and password.
                a method for sign-up, it should check for a non registered email

                after this user object need to created which has user_id, profile_id as attributes.

                a method to return movie, the movie should be added to watchlist with timestamp 0:00:00
                a method to update timestamp of above movie
                a method to add movies to wishlist / watchlater

2. use redis to cache user movie recommendations based on genre

                the next time user demands a movie, we should check whether movie is in cache before querying the db

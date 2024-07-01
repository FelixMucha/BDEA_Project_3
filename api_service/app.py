from fastapi import FastAPI, HTTPException, Request, Form, Query
from fastapi import File, UploadFile
from fastapi.responses import JSONResponse
from typing import List
from typing import Optional
from fastapi.responses import RedirectResponse
import os
import asyncio
from Graph_followers import TwitterGraph
from DB_tweet import Tweet_DB
import uvicorn
import requests
import logging
from uuid import UUID
from datetime import datetime


description = """
This API provides endpoints for interacting with Twitter data. The API has two main components: a Neo4j graph database and a Cassandra database. 
The Neo4j database is used to store information about Twitter users and their followers, while the Cassandra database is used to store tweets and likes.   
The API provides endpoints for querying the Neo4j database to get information about users and their followers, 
as well as endpoints for importing tweets into the Cassandra database and querying the Cassandra database to get tweets and likes. 
The API also provides endpoints for posting tweets, liking tweets, and updating the cache of a user's tweets in the Cassandra database.


To test the API following user_id can be used:
- 40981798 as the main user which posts tweets
- 279787626 as a follower of the main user which likes tweets and get the cache of the main user

"""

app = FastAPI(
    title="Twitter API",
    description=description,
    version="1.0.0"
)

logger = logging.getLogger("uvicorn")

# Neo4j connection details
uri = os.getenv("NEO4J_URI_1")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

graph = TwitterGraph(uri, user, password)

# TwitterDB connection details
tweet_db = Tweet_DB(hosts=['cassandra_node1'], keyspace='tweets')


def validate_tweet_date(tweet_date: str) -> datetime:
    try:
        # Attempt to parse the tweet_date string into a datetime object
        return datetime.strptime(tweet_date, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        # If parsing fails, raise an HTTPException that will be returned as a response
        raise HTTPException(status_code=400, detail="Invalid tweet_date format. Expected format: 'YYYY-MM-DDTHH:MM:SS.ssssss'")


@app.get("/", include_in_schema=False)
async def read_root():
    return RedirectResponse(url='/docs')

@app.post('/close_database_connection', tags=["neo4j"])
def close_database_connection():
    try:
        graph.close()
        return {"message": "Database connection closed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred closing the database connection")


@app.post('/clean_database', tags=["neo4j"])
def clean_database():
    try:
        graph.clean_database()
        return {"message": "Database cleaned successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred cleaning the database")
    

@app.post('/process_txt_file', tags=["neo4j"])
def process_txt_file(txt_file: str = 'data/twitter_combined.txt', limit: int = None):
    if not os.path.exists(txt_file):
        raise HTTPException(status_code=400, detail="File not found")
    try:
        graph.read_all_in_txt(txt_file, limit=limit)
        return {"message": "File got processed"}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")



@app.get('/users/with_most_followers', tags=["neo4j"])
def users_with_most_followers(limit: int = 100):
    try:
        result_list = graph.find_users_with_most_followers(limit=limit)
        return result_list
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")
    

@app.get('/users/follow_most', tags=["neo4j"])
def users_follow_most(limit: int = 100, followed_users: str = None):
    try:
        if followed_users:
            followed_users_list = followed_users.split(',')
        else:
            # get the followed users from graph.find_users_with_most_followers
            followed_users_list = graph.find_users_with_most_followers(limit=limit)
            followed_users_list = [record['user'] for record in followed_users_list]
        result_list = graph.find_users_which_follow_most(limit=limit, followed_users=followed_users_list)
        return result_list
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")
    

@app.get('/users/follow_stats', tags=["neo4j"])
def user_follow_stats(user_id: int = 40981798):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        follow_stats = graph.get_user_follow_stats(str(user_id))
        return follow_stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/users/user_follows', tags=["neo4j"])
def get_followed_users(user_id: int = 40981798):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        followed_users = graph.get_followed_users(str(user_id))
        return followed_users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/users/user_followers', tags=["neo4j"])
def get_followers(user_id: int = 40981798):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        followers = graph.get_followers(str(user_id))
        return followers
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/users/get_all', tags=["neo4j"])
def get_all_users():
    try:
        all_users = graph.get_all_users()
        return all_users
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")

@app.get('/status', tags=["all"])
def status():
    try:
        # Check Graph Database status
        try:
            user_count = graph.get_user_count()
            graph_status = f"available with {user_count} users"
        except Exception as e:
            graph_status = "unavailable"
        
        # Check Cassandra status
        try:
            tweet_count = tweet_db.get_tweet_count()
            cassandra_status = f"available with {tweet_count} tweets"
        except Exception as e:
            cassandra_status = "unavailable"
        
        return {
            "graph_status": graph_status,
            "cassandra_status": cassandra_status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")

# ----------------- Tweet DB -----------------


@app.post("/import_tweets", tags=["cassandra"])
def import_tweets(MAX_USERS: int = 20, csv_file: UploadFile = File(...), limit: int = None):
    try:
        user_nodes = users_with_most_followers(limit=MAX_USERS)
        tweet_db.setup_all_tables()

        # Save uploaded file to a temporary file, then import
        with open(csv_file.filename, "wb") as temp_file:
            content = csv_file.read()
            temp_file.write(content)
        
        tweet_db.import_csv(temp_file.name, user_nodes.json(), limit=limit)

        return {"message": "Tweets imported successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
@app.get("/tweets/get_tweets", tags=["cassandra"])
def get_tweets(user_id: int = 40981798, by_date: bool = True, filter_words: List[str] = Query(None), limit: int = 25):
    try:
        user_follows_data = get_followed_users(str(user_id))
        if not isinstance(user_follows_data, list):
            raise TypeError("Unexpected data format: user_follows_data is not a list")

        user_follows = [int(uf['followed']) for uf in user_follows_data if 'followed' in uf]

        newest_tweets = tweet_db.get_tweets_by_user_ids(user_follows, limit, filter_words=filter_words, by_likes=not by_date)
        return {'tweets': newest_tweets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

@app.post("/tweets/like_tweet", tags=["cassandra"], description="Likes a tweet by a user if tweet_id is not provided, the latest tweet by the follower is liked")
def like_tweet(user_id: int = 40981798, follower_id: int = 279787626, tweet_id: Optional[str] = None, 
               tweet_date: Optional[str] = Query(None, description="The date of the tweet in the format 'YYYY-MM-DDTHH:MM:SS.ssssss'")):
    try:
        user_followers = get_followers(str(user_id))
        user_followers = [int(uf['follower']) for uf in user_followers]
        if not tweet_id and not tweet_date:
            logger.info('The latest tweet is being liked')
            # get the tweet data
            query = "SELECT tweet_id, number_of_likes, tweet_date, content FROM tweets_cache WHERE follower_id = %s LIMIT 1"
            result = tweet_db.session.execute(query, (follower_id,))
            row = result.one()
            tweet_id = row.tweet_id
        else:
            logger.info('Getting the tweet data for the tweet_id')
            # get the tweet data
            query = """
                    SELECT number_of_likes, tweet_date, content
                    FROM tweets_cache
                    WHERE follower_id = %s AND tweet_date = %s AND tweet_id = %s
                    LIMIT 1
                """
            # Validate tweet_date format
            tweet_date = validate_tweet_date(tweet_date)
            tweet_id = UUID(tweet_id)
            result = tweet_db.session.execute(query, (follower_id, tweet_date, tweet_id))
            row = result.one()
        number_of_likes = row.number_of_likes
        tweet_date = row.tweet_date
        content = row.content
        # like the tweet
        tweet_db.like_tweet(user_id, follower_id, tweet_id, number_of_likes, tweet_date, content, user_followers)
        return {"message": "Tweet liked successfully"}
    except Exception as e:
        logger.error(f"Failed to like tweet: {e}")
        raise HTTPException(status_code=500, detail="An error occurred liking the tweet")


@app.post("/tweets/update_cache", 
          tags=["cassandra"], 
          description="Updates the cache for a user's tweets. If this is the first time you're using this endpoint for a user, "
                      "you must set `initial=True` to initialize the cache. Subsequent updates can use the default `initial=False`.")
def update_cache(user_id: int = 40981798, tweets: List[str] = Query(None), initial: bool = False, num_tweets: int = 25, sorted_by_likes: bool = False):
    # if tweets empty raise error that tweets are required if not inital
    if (tweets is None or len(tweets) == 0) and not initial:
        raise HTTPException(status_code=400, detail="tweets parameter is required if initial=False")
    try:
        if initial:
            # get tweets from the the followed users
            user_follows_data = get_followed_users(str(user_id))
            user_follows = [int(uf['followed']) for uf in user_follows_data if 'followed' in uf]
            tweets = tweet_db.get_tweets_by_user_ids(user_follows, num_tweets, filter_words=None, by_likes=sorted_by_likes)  
        tweet_db.update_cache(user_id, tweets)
        return {"message": "Cache updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred updating the cache")
    
@app.get("/tweets/get_tweets_from_cache", tags=["cassandra"])
def get_tweets_from_cache(user_id: int = 40981798, limit: int = 25):
    try:
        cached_tweets = tweet_db.get_tweets_from_cache(int(user_id), limit)
        return {'tweets': cached_tweets}
    except Exception as e:
        logger.error(f"Failed to get tweets from cache: {e}")
        raise HTTPException(status_code=500, detail="An error occurred getting tweets from cache")
    
@app.post("/tweets/init_random_likes", tags=["cassandra"])
def init_random_likes(user_id: int = 40981798, num_likes: int = 10, num_tweets: int = 10):
    try:
        results = get_all_users()
        possible_user_ids = [int(user) for user in results]
        # init_random_likes(self, user_id, liker_ids, n_likes=10, n_tweets=10):
        tweet_db.init_random_likes(user_id, possible_user_ids, num_likes, num_tweets)
        return {"message": "Random likes initialized successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred initializing random likes")

@app.get("/tweets/get_like_table", tags=["cassandra"])
def get_likes(limit: int = 5):
    try:
        query = f"SELECT * FROM tweet_likes LIMIT {limit}"
        result = tweet_db.session.execute(query)
        return [record for record in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred getting like table")

@app.post("/tweets/post_tweet", tags=["cassandra"])
def post_tweet(user_id: int = 40981798, tweet_content: str = Form(...)):
    try:
        user_followers = get_followers(str(user_id))
        user_followers = [int(uf['follower']) for uf in user_followers]
        tweet_db.post_tweet(user_id, tweet_content, user_followers)
        return {"message": "Tweet posted successfully"}
    except Exception as e:
        logger.error(f"Failed to get tweets from cache: {e}")
        raise HTTPException(status_code=500, detail="An error occurred posting the tweet")

@app.post("/tweets/clean_database", tags=["cassandra"])
def clean_database():
    try:
        tweet_db.clean_database()
        return {"message": "Database cleaned successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred cleaning the database")

@app.post("/tweets/setup_all_tables", tags=["cassandra"])
def setup_all_tables():
    try:
        tweet_db.setup_all_tables()
        return {"message": "Tables created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred creating the tables")

@app.post("/tweets/close_database_connection", tags=["cassandra"])
def close_database_connection():
    try:
        tweet_db.close()
        return {"message": "Database connection closed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred closing the database connection")


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
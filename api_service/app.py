from fastapi import FastAPI, HTTPException, Request, Form, Query
from fastapi import File, UploadFile
from typing import List
from fastapi.responses import RedirectResponse
import os
import asyncio
from Graph_followers import TwitterGraph
from DB_tweet import Tweet_DB
import uvicorn
import requests

app = FastAPI()

# Neo4j connection details
uri = os.getenv("NEO4J_URI_1")
user = os.getenv("NEO4J_USER")
password = os.getenv("NEO4J_PASSWORD")

graph = TwitterGraph(uri, user, password)

# TwitterDB connection details
tweet_db = Tweet_DB(hosts=['cassandra_node1'], keyspace='tweets')

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
def process_txt_file(txt_file: str = Form(...)):
    if not txt_file:
        raise HTTPException(status_code=400, detail="txt_file parameter is required")

    try:
        graph.read_all_in_txt(txt_file)
        return {"message": "File processing initiated"}
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
    if followed_users:
        followed_users_list = followed_users.split(',')
    else:
        raise HTTPException(status_code=400, detail="followed_users parameter is required")

    try:
        result_list = graph.find_users_which_follow_most(limit=limit, followed_users=followed_users_list)
        return result_list
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="An error occurred processing your request")
    

@app.get('/users/follow_stats', tags=["neo4j"])
def user_follow_stats(user_id: str = None):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        follow_stats = graph.get_user_follow_stats(user_id)
        return follow_stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get('/users/user_follows', tags=["neo4j"])
def get_followed_users(user_id: str = None):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        followed_users = graph.get_followed_users(user_id)
        return followed_users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get('/users/user_followers', tags=["neo4j"])
def get_followers(user_id: str):
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id parameter")
    try:
        followers = graph.get_followers(user_id)
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
def import_tweets(MAX_USERS: int, csv_file: UploadFile = File(...), limit: int = None):
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



if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
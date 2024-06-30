import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import requests
# from uuid import uuid4 as uuid
import uuid
from datetime import datetime
from cassandra.util import uuid_from_time

class Tweet_DB:
    def __init__(self, hosts, keyspace, auth_provider=None):
        self.keyspace = keyspace
        if auth_provider:
            self.cluster = Cluster(hosts, auth_provider=auth_provider)
        else:
            self.cluster = Cluster(hosts)
        
        self.session = self.cluster.connect()  # Connect without specifying a keyspace
        self.host_name = hosts[0]

        # Check if the keyspace exists
        keyspace_exists = self.session.execute("""
            SELECT * FROM system_schema.keyspaces WHERE keyspace_name = %s
        """, (keyspace,)).one()

        if not keyspace_exists:
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
                AND durable_writes = true;
            """)

        self.session.set_keyspace(keyspace)  # Connect to the keyspace

        # date sorted table
        self.setup_all_tables
    
    def setup_all_tables(self):
        self.setup_initial_table(sorted_by_date=True)
        self.setup_initial_table(sorted_by_date=False)
        self.setup_cache_table()
        self.setup_likes_table()

    def setup_initial_table(self, sorted_by_date=True):
        if sorted_by_date:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tweets_by_date (
                user_id int,
                tweet_id uuid,
                tweet_date timestamp,
                content text,
                number_of_likes int,
                PRIMARY KEY (user_id, tweet_date, tweet_id)
            ) WITH CLUSTERING ORDER BY (tweet_date DESC, tweet_id DESC);
            """
        else:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tweets_by_likes (
                user_id int,
                number_of_likes int,
                tweet_id uuid,
                tweet_date timestamp,
                content text,
                PRIMARY KEY (user_id, number_of_likes, tweet_id)
            ) WITH CLUSTERING ORDER BY (number_of_likes DESC, tweet_id DESC);
            """
        self.session.execute(create_table_query)

    def setup_cache_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tweets_cache (
            follower_id int,
            tweet_id uuid,
            tweet_date timestamp,
            content text,
            number_of_likes int,
            PRIMARY KEY (follower_id, tweet_date, tweet_id)
        ) WITH CLUSTERING ORDER BY (tweet_date DESC, tweet_id DESC);
        """
        self.session.execute(create_table_query)
    
    def setup_likes_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS tweet_likes (
            tweet_id uuid,
            user_id int,
            PRIMARY KEY (tweet_id, user_id)
        ) WITH CLUSTERING ORDER BY (user_id ASC);
        """
        self.session.execute(create_table_query)

    def init_random_likes(self, user_id, n_likes=10, n_tweets=10):
        query = "SELECT tweet_id, number_of_likes, tweet_date FROM tweets_by_date WHERE user_id = %s ORDER BY tweet_date DESC"
        result = self.session.execute(query, (user_id,))
        tweet_ids = []
        number_of_likes = []
        tweet_dates = []
        for re in result:
            tweet_ids.append(re.tweet_id)
            number_of_likes.append(re.number_of_likes)
            tweet_dates.append(re.tweet_date)

        tweet_ids = tweet_ids[:n_tweets]
        number_of_likes = number_of_likes[:n_tweets]
        tweet_dates = tweet_dates[:n_tweets]

        # Get all possible user_ids
        results = requests.get(f"http://{self.host_name}:5000/users/get_all")
        user_ids = [int(user) for user in results.json()]

        for tweet_idx, tweet_id in enumerate(tweet_ids):
            print(f'For tweet: {tweet_idx + 1} of {len(tweet_ids)}, likes are being added')
            # Get a random list of user_ids with length of n_likes
            random_user_ids = np.random.choice(user_ids, min(n_likes, len(user_ids)), replace=False)
            
            for user_id in random_user_ids:
                insert_query = "INSERT INTO tweet_likes (tweet_id, user_id) VALUES (%s, %s)"
                self.session.execute(insert_query, (tweet_id, user_id))
    
    def like_tweet(self, author_id, liker_id, tweet_id, number_of_likes, tweet_date, content):
        """
        # check if the user has already liked the tweet
        query = "SELECT COUNT(*) FROM tweet_likes WHERE tweet_id = %s AND user_id = %s"
        result = self.session.execute(query, (tweet_id, user_id))
        count = result.one().count
        #rint(count)
        if count > 0:
            print("User has already liked the tweet")
            return
        """
        number_of_likes += 1
        # insert the like into the tweet_likes table
        insert_query = "INSERT INTO tweet_likes (tweet_id, user_id) VALUES (%s, %s)"
        self.session.execute(insert_query, (tweet_id, liker_id))
        # update the tweets_by_date table
        update_query = "UPDATE tweets_by_date SET number_of_likes = %s WHERE user_id = %s AND tweet_date = %s AND tweet_id = %s"
        self.session.execute(update_query, (number_of_likes, author_id, tweet_date, tweet_id))
        # delete the old tweet from tweets_by_likes table
        delete_query = "DELETE FROM tweets_by_likes WHERE user_id = %s AND tweet_id = %s AND number_of_likes = %s"
        self.session.execute(delete_query, (author_id, tweet_id, number_of_likes-1))
        # insert the new tweet into the tweets_by_likes table
        insert_query = """
        INSERT INTO tweets_by_likes (user_id, number_of_likes, tweet_id, tweet_date, content)
        VALUES (%s, %s, %s, %s, %s)
        """
        self.session.execute(insert_query, (author_id, number_of_likes, tweet_id, tweet_date, content))

        # update the cache
        # get all followers of the author
        user_followers = requests.get(f"http://{self.host_name}:5000/users/user_followers", params={"user_id": str(author_id)})
        user_followers = user_followers.json()
        user_followers = [int(uf['follower']) for uf in user_followers]
        for follower_id in user_followers:
            update_cache_query = "UPDATE tweets_cache SET number_of_likes = %s WHERE follower_id = %s AND tweet_date = %s AND tweet_id = %s"
            self.session.execute(update_cache_query, (number_of_likes, follower_id, tweet_date, tweet_id))

    def get_tweet_count(self):
        query = "SELECT COUNT(*) FROM tweets_by_date"
        result = self.session.execute(query)
        return result.one().count


    def clean_database(self, keyspace_name='tweets', tables=None):
        print("Cleaning database...")
        if tables is None:
            # Query to get the list of tables in the keyspace
            query_tables = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace_name}';"
            tables = self.session.execute(query_tables)

        # Iterate over the tables and drop each one
        for row in tables:
            table_name = row.table_name
            drop_table_query = f"DROP TABLE IF EXISTS {keyspace_name}.{table_name};"
            try:
                self.session.execute(drop_table_query)
                print(f"Dropped table {table_name}")
            except Exception as e:
                print(f"Error dropping table {table_name}: {e}")

    def load_data(self, query, parameters=None):
        if parameters is None:
            parameters = {}
        self.session.execute(query, parameters)

    def create_or_update_user_mapping(self, user_id, username):
        """
        Create or update the user mapping table with the given user_id and username.
        """
        # Create the user_mapping table if it does not exist
        self.session.execute("""CREATE TABLE IF NOT EXISTS user_mapping (user_id int PRIMARY KEY, username text)""")

        # Check if the user_id already exists in the mapping table
        check_query = "SELECT COUNT(*) FROM user_mapping WHERE user_id = %s"
        result = self.session.execute(check_query, (user_id,))
        count = result[0].count  # Access the count from the first (and only) row in the result set
        
        if count == 0:
            # If the user_id does not exist, insert a new record
            insert_query = "INSERT INTO user_mapping (user_id, username) VALUES (%s, %s)"
            self.session.execute(insert_query, (user_id, username))
        else:
            # If the user_id exists, update the existing record (if you want to keep the table updated with the latest username)
            update_query = "UPDATE user_mapping SET username = %s WHERE user_id = %s"
            self.session.execute(update_query, (username, user_id))

    def get_username_from_user_id(self, user_id):
        """
        Retrieve the username for a given user_id from the user_mapping table.
        """
        query = "SELECT username FROM user_mapping WHERE user_id = %s"
        result = self.session.execute(query, (user_id,))
        # if result is empty, return None
        if not result:
            return None
        return result[0].username
    
    def get_user_id_from_username(self, username):
        """
        Retrieve the user_id for a given username from the user_mapping table.
        """
        query = "SELECT user_id FROM user_mapping WHERE username = %s"
        result = self.session.execute(query, (username,))
        if not result:
            return None
        return result[0].user_id

    def import_csv(self, csv_file, user_ids, limit=None):
        print(f"Importing data from {csv_file}")
        df = pd.read_csv(csv_file)
        if limit:
            df = df.iloc[:limit]

        user_ids_li = [int(user['user']) for user in user_ids]
        user_ids_dict = {user_id: '' for user_id in user_ids_li}

        row_count = len(df)
        print(f"Inserting {row_count} rows into the database")
        start_time = datetime.now()
        for idx, row in df.iterrows():
            tweet_date = datetime.strptime(row['date_time'], '%d/%m/%Y %H:%M')
            #tweet_id = uuid_from_time(tweet_date)  # Generate UUID based on tweet_date
            # = (row['user_id'], row['number_of_likes'], tweet_id, row['tweet_date'], row['content']) if table_type == 'likes' else (row['user_id'], tweet_id, row['tweet_date'], row['content'], row['number_of_likes'])
            
            #self.session.execute(insert_query, values)

            if idx % 1000 == 0:
                end_time = datetime.now()
                time_diff = (end_time - start_time).total_seconds()
                print(f"Inserted {idx}/{row_count} rows in time {round(time_diff, 2)}")
                start_time = datetime.now()

            # check if user name is in user_ids_dict values
            if row['author'] in user_ids_dict.values():
                # get the key where the value is equal to the row['author']
                user_id = [key for key, value in user_ids_dict.items() if value == row['author']][0]
            else:
                # get a key where the value is ''
                user_id = [key for key, value in user_ids_dict.items() if value == ''][0]
                user_ids_dict[user_id] = row['author']

            tweet_id = uuid.uuid4()
            # write to tweets table by likes
            insert_query = """
                        INSERT INTO tweets_by_likes (user_id, number_of_likes, tweet_id, tweet_date, content)
                        VALUES (%s, %s, %s, %s, %s)
                        """
            values = (user_id, row['number_of_likes'], tweet_id,  tweet_date, row['content'])
            self.session.execute(insert_query, values)
            # write to tweets table by date
            insert_query = """
                            INSERT INTO tweets_by_date (user_id, tweet_id, tweet_date, content, number_of_likes)
                            VALUES (%s, %s, %s, %s, %s)
                            """
            values = (user_id, tweet_id, tweet_date, row['content'], row['number_of_likes'])
            self.session.execute(insert_query, values)

        for user_id, user_name in user_ids_dict.items():
            self.create_or_update_user_mapping(user_id, user_name)


    def get_tweets_by_user_ids(self, user_ids, n, filter_words=None, by_likes=False):
        tweets = []
        table_name = "tweets_by_likes" if by_likes else "tweets_by_date"
        for user_id in user_ids:
            query = f"""
                SELECT user_id, tweet_id, tweet_date, content, number_of_likes
                FROM {table_name}
                WHERE user_id = %s
                LIMIT %s
            """
            results = self.session.execute(query, (user_id, n))
            for row in results:
                if filter_words:
                    tweet_text = row.content.lower()
                    if all(word.lower() in tweet_text for word in filter_words):
                        tweets.append(row)
                else:
                    tweets.append(row)

        # Determine the sorting key based on the query type
        if by_likes:
            sort_key = lambda x: (x.number_of_likes, x.tweet_id)
        else:
            sort_key = lambda x: (x.tweet_date, x.tweet_id)

        # Sort the tweets based on the determined sort key in descending order
        tweets.sort(key=sort_key, reverse=True)

        # Return the top n tweets
        return tweets[:n]

    def update_cache(self, user_id, tweets, n=25, initial=False, new_tweet=False):
        if initial:
            # get tweets from the the followed users
            user_follows = requests.get(f"http://{self.host_name}:5000/users/user_follows", params={"user_id": str(user_id)})
            user_follows = user_follows.json()
            user_follows = [int(uf['followed']) for uf in user_follows]
            tweets = self.get_tweets_by_user_ids(user_follows, n, filter_words=None, by_likes=False)

        for tweet in tweets:
            insert_query = """
                INSERT INTO tweets_cache (follower_id, tweet_id, tweet_date, content, number_of_likes)
                VALUES (%s, %s, %s, %s, %s)
            """
            if new_tweet:
                values = (user_id, tweet['tweet_id'], tweet['tweet_date'], tweet['content'], tweet['number_of_likes'])
            else:
                values = (user_id, tweet.tweet_id, tweet.tweet_date, tweet.content, tweet.number_of_likes)
            self.session.execute(insert_query, values)
        
        # Get the current number of tweets in the cache
        query = "SELECT COUNT(*) FROM tweets_cache WHERE follower_id = %s"
        result = self.session.execute(query, (user_id,))
        count = result.one().count

        # If the cache has more than n tweets, remove the oldest tweets
        if count > n:
            # Get the tweet_date of the nth oldest tweet
            query = f"""
                SELECT tweet_date FROM tweets_cache WHERE follower_id = %s
                ORDER BY tweet_date ASC LIMIT 1 OFFSET %s
            """
            result = self.session.execute(query, (user_id, n-1))
            oldest_tweet_date = result.one().tweet_date

            # Delete all tweets older than the nth oldest tweet
            query = f"""
                DELETE FROM tweets_cache WHERE follower_id = %s AND tweet_date < %s
            """
            self.session.execute(query, (user_id, oldest_tweet_date))
    
    def get_tweets_from_cache(self, user_id, n=25):
        query = "SELECT * FROM tweets_cache WHERE follower_id = %s LIMIT %s"
        result = self.session.execute(query, (user_id, n))
        return result
        

    def post_tweet(self, user_id, tweet_text):
        tweet_id = uuid.uuid4()
        tweet_date = datetime.now()
        tweet = {
            'tweet_id': tweet_id, 
            'tweet_date': tweet_date, 
            'content': tweet_text,
             'number_of_likes': 0
        }
        # write to tweets table by date
        insert_query = """
            INSERT INTO tweets_by_date (user_id, tweet_id, tweet_date, content, number_of_likes)
            VALUES (%s, %s, %s, %s, 0)
        """
        values = (user_id, tweet_id, tweet_date, tweet_text)
        self.session.execute(insert_query, values)
        # write to tweets table by likes
        insert_query = """
            INSERT INTO tweets_by_likes (user_id, number_of_likes, tweet_id, tweet_date, content)
            VALUES (%s, 0, %s, %s, %s)
        """
        values = (user_id, tweet_id, tweet_date, tweet_text)
        self.session.execute(insert_query, values)
        # insert into cache in fan out style
        user_followers = requests.get(f"http://{self.host_name}:5000/users/user_followers", params={"user_id": str(user_id)})
        user_followers = user_followers.json()
        user_followers = [int(uf['follower']) for uf in user_followers]

        for follows_id in user_followers:
            self.update_cache(follows_id, [tweet], initial=False, new_tweet=True)



    def close(self):
        self.cluster.shutdown()


# Example usage
if __name__ == "__main__":

        # TODO: get the nodes reliable to work with the db


        # TODO: self.host_name replace so it can be used only in app -> move it out of the functions
        # TODO: add api routes to the query_service (clean_db, 
        #                                           init_random_likes, 
        #                                           get like table with limit,
        #                                           cached from the user
        #                                           post a tweet
        #                                           cahced from a follower
        #                                           like a tweet
        #                                           get tweet from db for a follower
        #                                           get tweet from cache for a follower
        #                                           )
        # TODO: instead flag after db type flag for presentation
        # TODO: reset db and load all data after all finished

        # TODO: add more comments
    
    MAX_USERS = 20

    # for local testing
    tweet_db = Tweet_DB(hosts=['127.0.0.1'], keyspace='tweets')
    # for in docker testing
    # tweet_db = Tweet_DB(hosts=['cassandra_node1'], keyspace='tweets')
    user_id = 40981798 #20747847

    """
    # read data from csv file
    user_nodes = requests.get("http://127.0.0.1:5000/users/with_most_followers", params={"limit": f"{MAX_USERS}"})
    
    #tweet_db.clean_database()
    tweet_db.setup_all_tables()

    data_path = "data/tweets.csv"
    tweet_db.import_csv(data_path, user_nodes.json(), limit=None)
    """
    
    """
    print('-' * 100)
    print('Randomly like tweets of a user')
    print('-' * 100)
    # tweet_db.init_random_likes(user_id)
    
    
    # print first 10 likes
    query = "SELECT * FROM tweet_likes LIMIT 10"
    result = tweet_db.session.execute(query)
    for row in result:
        print(row)
    """
        

    """
    print('-' * 100)
    print('Print the newest tweets from the users that the user follows')
    print('-' * 100)
    user_follows = requests.get("http://127.0.0.1:5000/users/user_follows", params={"user_id": str(user_id)})
    user_follows = user_follows.json()
    user_follows = [int(uf['followed']) for uf in user_follows]

    print('By date')
    newest_tweets = tweet_db.get_tweets_by_user_ids(user_follows, 25, filter_words=None, by_likes=False)
    for tweet in newest_tweets:
        print(tweet)
    
    print('By likes')
    newest_tweets = tweet_db.get_tweets_by_user_ids(user_follows, 25, filter_words=None, by_likes=True)
    for tweet in newest_tweets:
        print(tweet)
    """

    """
    print('-' * 100)
    print('Print the cached from the user that the user follows')
    print('-' * 100)
    tweet_db.update_cache(user_id, tweets=None, initial=True)
    cached_tweets = tweet_db.get_tweets_from_cache(user_id, 25)
    for tweet in cached_tweets:
        print(tweet)
    """

    """
    print('-' * 100)
    print('Post a tweet and get the newest posts from the user from db')
    print('-' * 100)
    tweet_db.post_tweet(user_id, "TEST TWEET")
    # get newest posts from db from the user
    newest_tweets = tweet_db.get_tweets_by_user_ids([user_id], 4, filter_words=None, by_likes=False)
    for tweet in newest_tweets:
        print(tweet)
    """

    """
    print('-' * 100)
    print('Print the cached from the user who follows the user')
    print('-' * 100)
    # get the followers of the user
    user_followers = requests.get("http://127.0.0.1:5000/users/user_followers", params={"user_id": str(user_id)})
    user_followers = user_followers.json()
    user_followers = [int(uf['follower']) for uf in user_followers]
    follower_id = user_followers[4]
    # update the cache
    tweet_db.update_cache(follower_id, tweets=None, initial=True)
    # get the cached tweets
    cached_tweets = tweet_db.get_tweets_from_cache(follower_id, 25)
    for tweet in cached_tweets:
        print(tweet)
    """

    
    print('-' * 100)
    print('Like a tweet')
    print('-' * 100)
    # get the followers of the user
    user_followers = requests.get("http://127.0.0.1:5000/users/user_followers", params={"user_id": str(user_id)})
    user_followers = user_followers.json()
    user_followers = [int(uf['follower']) for uf in user_followers]
    follower_id = user_followers[4]
    # get the tweet_id from the cache
    query = "SELECT tweet_id, number_of_likes, tweet_date, content FROM tweets_cache WHERE follower_id = %s LIMIT 1"
    result = tweet_db.session.execute(query, (follower_id,))
    row = result.one()
    # get the tweet data
    tweet_id = row.tweet_id
    number_of_likes = row.number_of_likes
    tweet_date = row.tweet_date
    content = row.content
    # like the tweet
    tweet_db.like_tweet(user_id, follower_id, tweet_id, number_of_likes, tweet_date, content)

    print('-' * 100)
    print('Print the cached from the user who follows the user')
    print('-' * 100)
    # get the tweet from the cache
    cached_tweets = tweet_db.get_tweets_from_cache(follower_id, 25)
    for tweet in cached_tweets:
        print(tweet)
    
    print('-' * 100)
    print('Print the tweet from the user in db')
    print('-' * 100)
    # get the tweet from the tweets_by_datetable
    query = "SELECT * FROM tweets_by_date WHERE user_id = %s LIMIT 1"
    #user_id = 40981798
    result = tweet_db.session.execute(query, (user_id,))
    for row in result:
        print(row)

    
    """
    # update cache
    # tweet_db.update_cache(user_id, newest_tweets)

    print('-' * 100)

    # get tweets from cache
    query = "SELECT * FROM tweets_cache WHERE follower_id = %s"
    result = tweet_db.session.execute(query, (user_id,))
    for row in result:
        print(row)
    """




















    """
    # print user_mapping table
    query = "SELECT * FROM user_mapping"
    result = tweet_db.session.execute(query)
    for row in result:
        print(row)
    """
    # print how many unique usernames are in the database

    # get column names
    """
    query = "SELECT * FROM tweets LIMIT 1"
    result = tweet_db.session.execute(query)
    column_names = result.column_names
    print(column_names)
    """

    # Query to count unique usernames
    # Fetch distinct usernames
    """
    query = "SELECT user_name FROM tweets"
    result = tweet_db.session.execute(query)
    names = set([row.user_name for row in result])
    unique_user_count = len(set(row.user_name for row in result))
    print(f"Number of unique usernames in the database: {names}")


    user_nodes = requests.get("http://127.0.0.1:5000/users/with_most_followers", params={"limit": f"{unique_user_count}"})
    #print(user_nodes.text, user_nodes.json())

    # Update the Cassandra table with the user IDs
    tweet_db.map_user_id_to_username(names, user_nodes.json())
    """



    # query data
    # print df head
    """
    query = "SELECT * FROM tweets LIMIT 5"
    result = tweet_db.session.execute(query)
    for row in result:
        print(row)
    """
    """
    # get newest posts
    newest_posts = tweet_db.get_posts_from_followed('katyperry', by_likes=False, limit=25)
    for post in newest_posts:
        print(post)
    """
    """
    user_follows = requests.get("http://127.0.0.1:5000/users/user_follows", params={"user_id": "20747847"})
    user_follows = user_follows.json()
    user_follows = [int(uf['follows']) for uf in user_follows]
    #print(user_follows.text)
    print('-' * 100)
    tweets = tweet_db.get_tweets_from_followed(user_follows.json(), by_likes=True, limit=25, filter_words=['love', 'hate'])
    for tweet in tweets:
        print(tweet.tweet_date, tweet.number_of_likes, tweet.user_name, tweet.tweet_text)
    
    tweet_db.post_tweet(20747847, "This is a test tweet", user_follows)

    """

    tweet_db.close()
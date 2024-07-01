from neo4j import GraphDatabase
import time as time

class TwitterGraph:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()
    
    def get_user_count(self):
        with self.driver.session() as session:
            result = session.run("MATCH (u:User) RETURN COUNT(u) AS userCount")
            return result.single()[0]

    def create_follows_relationship(self, follower, followed):
        with self.driver.session() as session:
            session.execute_write(self._create_and_link, follower, followed)

    @staticmethod
    def _create_and_link(tx, follower, followed):
        query = (
            "MERGE (a:User {id: $follower}) "
            "MERGE (b:User {id: $followed}) "
            "MERGE (a)-[:FOLLOWS]->(b)"
        )
        tx.run(query, follower=follower, followed=followed)

    def clean_database(self):
        print("Cleaning database...")
        with self.driver.session() as session:
            session.execute_write(self._delete_all)

    @staticmethod
    def _delete_all(tx):
        query = "MATCH (n) DETACH DELETE n"
        tx.run(query)
    
    def read_all_in_txt(self, txt_file, limit=None):
        try:
            with open(txt_file, "r") as file:
                # print how many lines exist in the file
                total_lines = 2420766 #len(file.readlines())
                #print(f"Total lines in file: {total_lines}")
                start_time = time.time()
                for line_idx, line in enumerate(file):
                    if limit is not None and line_idx >= limit:
                        break
                    if line_idx % 1000 == 0:
                        stop_time = time.time()
                        print(f"Processed {line_idx}/{total_lines} lines in time: {round(stop_time - start_time, 2)} seconds.")
                        start_time = time.time()
                    follower, followed = line.strip().split()
                    self.create_follows_relationship(follower, followed)
        except Exception as e:
            print(f"Error reading file: {e}")
    
    def get_all_users(self):
        def process_result(tx):
            query = "MATCH (u:User) RETURN u.id AS user"
            result = tx.run(query)
            return [record["user"] for record in result]

        with self.driver.session() as session:
            result_list = session.execute_read(process_result)
        
        return result_list

    def find_users_with_most_followers(self, limit=100):
        def process_result(tx, limit=limit):
            query = (
                "MATCH (a:User)<-[:FOLLOWS]-(b) "
                "RETURN a.id AS user, count(b) AS followersCount "
                "ORDER BY followersCount DESC "
                f"LIMIT {limit}"
            )
            result = tx.run(query)
            return [record.data() for record in result]

        with self.driver.session() as session:
            result_list = session.execute_read(process_result)
        
        return result_list
    
    def find_users_which_follow_most(self, limit=100, followed_users=None):
        def process_result(tx, limit=limit, followed_users=followed_users):
            # Convert the list of followed users to a string format suitable for the Cypher query
            followed_users_str = str(followed_users).replace('[', '').replace(']', '')
            query = (
                "MATCH (a:User)-[:FOLLOWS]->(b) "
                "WHERE b.id IN [" + followed_users_str + "] "
                "RETURN a.id AS user, count(b) AS followsCount "
                "ORDER BY followsCount DESC "
                f"LIMIT {limit}"
            )
            result = tx.run(query)
            return [record.data() for record in result]

        if followed_users is None or not followed_users:
            raise ValueError("followed_users must be a non-empty list of user ids")

        with self.driver.session() as session:
            result_list = session.execute_read(process_result)
        
        return result_list

    def get_user_follow_stats(self, user_id):
        def process_result(tx, user_id=user_id):
            # Query to count how many users this user follows
            follows_query = (
                "MATCH (a:User {id: $user_id})-[:FOLLOWS]->(b) "
                "RETURN count(b) AS followsCount"
            )
            follows_result = tx.run(follows_query, user_id=user_id).single()["followsCount"]

            # Query to count how many followers this user has
            followers_query = (
                "MATCH (a:User {id: $user_id})<-[:FOLLOWS]-(b) "
                "RETURN count(b) AS followersCount"
            )
            followers_result = tx.run(followers_query, user_id=user_id).single()["followersCount"]

            return {"followsCount": follows_result, "followersCount": followers_result}

        with self.driver.session() as session:
            result = session.execute_read(process_result)
        
        return result
    
    def get_followed_users(self, user_id):
        def process_result(tx, user_id=user_id):
            query = (
                "MATCH (a:User {id: $user_id})-[:FOLLOWS]->(b) "
                "RETURN b.id AS followed"
            )
            result = tx.run(query, user_id=user_id)
            return [record.data() for record in result]

        with self.driver.session() as session:
            result_list = session.execute_read(process_result)
        
        return result_list
    
    def get_followers(self, user_id):
        def process_result(tx, user_id=user_id):
            query = (
                "MATCH (a:User {id: $user_id})<-[:FOLLOWS]-(b) "
                "RETURN b.id AS follower"
            )
            result = tx.run(query, user_id=user_id)
            return [record.data() for record in result]

        with self.driver.session() as session:
            result_list = session.execute_read(process_result)
        
        return result_list
    

if __name__ == "__main__":

    # Update these variables with your Neo4j connection details
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "testtest"
    graph = TwitterGraph(uri, user, password)
    
    # read all data from txt file
    '''txt_file = "data/twitter_combined.txt"
    graph.clean_database()
    graph.read_all_in_txt(txt_file)'''
    
    print('-' * 100)
    print('get users which has most followers')
    print('-' * 100)
    # Find and print the user with the most followers
    most_followed = graph.find_users_with_most_followers(limit=25)
    for record in most_followed:
        print(f"User {record['user']} has {record['followersCount']} followers")


    print('-' * 100)
    print('get users which followers the most users from the list above')
    print('-' * 100)
    # get the ids from most most_followed
    follower_str = [record['user'] for record in most_followed]
    # Find and print the user which follows the most users
    follows_most = graph.find_users_which_follow_most(limit=25, followed_users=follower_str)
    for record in follows_most:
        print(f"User {record['user']} follows {record['followsCount']} users")

    print('-' * 100)
    print('get example user followers and followed amount')
    print('-' * 100)
    follower_id = follower_str[0]
    user_info = graph.get_user_follow_stats(follower_id)
    print(f"User: {follower_id}, follows count: {user_info['followsCount']}, followers count: {user_info['followersCount']}")



    print('-' * 100)
    print('get example user followers and followed amount')
    print('-' * 100)
    get_followed = graph.get_followed_users(follower_id)
    print(f"User: {follower_id}, follows: {get_followed}")

    print('-' * 100)
    print('get example user followers')
    print('-' * 100)
    get_followers = graph.get_followers(follower_id)
    print(f"User: {follower_id}, followers: {get_followers}")


    graph.close()

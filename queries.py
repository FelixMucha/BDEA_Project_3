from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# Verbindung zur Cassandra-Datenbank herstellen
cluster = Cluster(['localhost'])
session = cluster.connect('twitter_clone')

'''
# Verbindung zur Neo4j-Datenbank herstellen
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(username, password))
'''

# Verbindung zur Neo4j-Datenbank herstellen
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
driver = GraphDatabase.driver(uri, auth=(username, password))


def get_top_100_followers():
    # Cassandra-Abfrage: Die 100 Benutzer mit den meisten Followern abrufen
    rows = session.execute("SELECT username, followers FROM users")
    user_followers = [(row.username, len(row.followers) if row.followers is not None else 0) for row in rows]
    top_100_followers = sorted(user_followers, key=lambda x: x[1], reverse=True)[:100]
    return top_100_followers


def get_users_following_top_100():
    # Die Top-100-Benutzer mit den meisten Followern abrufen
    top_100_followers = [username for username, _ in get_top_100_followers()]
    # Benutzer abrufen, die den Top-100-Benutzern folgen
    query = "SELECT username, following FROM users"
    rows = session.execute(query)
    users_following_top_100 = []
    for row in rows:
        if row.username in top_100_followers and row.following is not None:
            users_following_top_100.extend(row.following)
    return users_following_top_100[:100]

if __name__ == "__main__":
    print(get_top_100_followers())
    print(get_users_following_top_100())
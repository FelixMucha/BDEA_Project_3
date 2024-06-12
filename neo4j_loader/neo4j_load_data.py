from neo4j import GraphDatabase
import csv
import time

# Warte, bis die Neo4j-Datenbank verfügbar ist
time.sleep(30)

# Verbindung zu Neo4j herstellen
uri = "bolt://neo4j:7687"
username = "neo4j"
password = "password"

driver = GraphDatabase.driver(uri, auth=(username, password))

def create_author(tx, author):
    tx.run("MERGE (a:Author {name: $author})", author=author)

def create_tweet(tx, tweet_id, content, country, date_time, language, latitude, longitude, number_of_likes, number_of_shares):
    tx.run("""
    MERGE (t:Tweet {id: $tweet_id})
    SET t.content = $content, t.country = $country, t.date_time = $date_time, t.language = $language,
        t.latitude = $latitude, t.longitude = $longitude, t.number_of_likes = $number_of_likes, t.number_of_shares = $number_of_shares
    """, tweet_id=tweet_id, content=content, country=country, date_time=date_time, language=language, latitude=latitude, longitude=longitude, number_of_likes=number_of_likes, number_of_shares=number_of_shares)

def create_author_tweet_relationship(tx, author, tweet_id):
    tx.run("""
    MATCH (a:Author {name: $author})
    MATCH (t:Tweet {id: $tweet_id})
    MERGE (a)-[:POSTED]->(t)
    """, author=author, tweet_id=tweet_id)

def load_data(file_path):
    with driver.session() as session:
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                author = row['author']
                tweet_id = row['id']
                content = row['content']
                country = row['country']
                date_time = row['date_time']
                language = row['language']
                latitude = row['latitude']
                longitude = row['longitude']
                number_of_likes = row['number_of_likes']
                number_of_shares = row['number_of_shares']
                
                session.write_transaction(create_author, author)
                session.write_transaction(create_tweet, tweet_id, content, country, date_time, language, latitude, longitude, number_of_likes, number_of_shares)
                session.write_transaction(create_author_tweet_relationship, author, tweet_id)

if __name__ == "__main__":
    load_data("/app/twitt.csv")
    driver.close()

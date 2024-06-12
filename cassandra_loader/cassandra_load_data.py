from cassandra.cluster import Cluster
import time

# Warte, bis die Cassandra-Datenbank verfügbar ist
time.sleep(30)

# Verbindung zu Cassandra herstellen
cluster = Cluster(['cassandra'])
session = cluster.connect()

# Keyspace erstellen und verwenden
session.execute("""
CREATE KEYSPACE IF NOT EXISTS twitter_clone 
WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.execute("USE twitter_clone")

# Tabellen erstellen
session.execute("""
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    followers LIST<TEXT>,
    following LIST<TEXT>
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS followers (
    username TEXT,
    follows TEXT,
    PRIMARY KEY (username, follows)
)
""")

# Daten aus der Datei laden und in die Datenbank einfügen
with open('/app/twitter_combined.txt', 'r') as f:
    for line in f:
        follower, followed = line.strip().split()
        
        # Follower-Beziehung in der followers-Tabelle speichern
        session.execute("""
        INSERT INTO followers (username, follows) VALUES (%s, %s)
        """, (follower, followed))
        
        # Following-Liste des Followers aktualisieren
        session.execute("""
        UPDATE users SET following = following + [%s] WHERE username = %s
        """, (followed, follower))
        
        # Follower-Liste des Gefolgten aktualisieren
        session.execute("""
        UPDATE users SET followers = followers + [%s] WHERE username = %s
        """, (follower, followed))
        
        # Sicherstellen, dass beide Benutzer in der users-Tabelle existieren
        session.execute("""
        INSERT INTO users (username) VALUES (%s)
        IF NOT EXISTS
        """, (follower,))
        
        session.execute("""
        INSERT INTO users (username) VALUES (%s)
        IF NOT EXISTS
        """, (followed,))

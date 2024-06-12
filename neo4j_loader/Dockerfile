# Basis-Image
FROM neo4j:latest

# Erstellen des Arbeitsverzeichnisses
WORKDIR /app

# Kopieren des Skripts und der Datendatei in das Image
COPY neo4j_load_data.py /app/neo4j_load_data.py
COPY tweets.csv /app/tweets.csv

# Python und notwendige Pakete installieren
RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install neo4j

# Neo4j-Server starten und das Skript ausführen
CMD ["sh", "-c", "sleep 30 && python3 /app/neo4j_load_data.py && tail -f /dev/null"]
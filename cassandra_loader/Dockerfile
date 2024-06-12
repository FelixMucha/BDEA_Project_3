# Basis-Image
FROM cassandra:latest

# Erstellen des Arbeitsverzeichnisses
WORKDIR /app

# Kopieren des Skripts und der Datendatei in das Image
COPY cassandra_load_data.py /app/cassandra_load_data.py
COPY twitter_combined.txt /app/twitter_combined.txt

# Python und notwendige Pakete installieren
RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install cassandra-driver

# Cassandra starten und das Skript ausführen
CMD ["sh", "-c", "sleep 30 && python3 /app/cassandra_load_data.py && tail -f /dev/null"]

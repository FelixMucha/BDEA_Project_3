# Big Data Engineering and Analytics Project 3
This repository contains the source code for an exercise in the field of Big Data Engineering and Analytics as part of the Master of Computer Science, SS2024.

## Project Goal
This project implements a simple social network using NoSQL databases. Docker and Docker-Compose are used to deploy the database across two containers. The main functionalities of the system include storing and querying posts, follower relationships, and likes. The task includes several specific queries that the system should support


## Project Structure
1. docker-compose.yml: Docker-Compose file to start up the NoSQL databases
2. Dockerfile: Dockerfile for creating the database containers
3. data_preprocessing.py: Script for preprocessing and loading data into the database
4. queries.py: Python file with the required queries


## Technologies used
Docker - Docker-compose
Python
NoSql Database: Neo4J and Cassandra


## Installation Guide
1. Start Database Containers: `docker-compose up -d`
2. Load the Data into the Database: `python3 data_processing.py`
3. Excecute the queries:


## Queries
The supported queries are detailed in queries.py and include:
1. Listing posts made by a specific account
2. Finding the 100 accounts with the most followers
3. Finding the 100 accounts that follow the most of the accounts found in point 2
4. Information for the personal homepage of any account:
   Number of followers
   Number of followed accounts
   25 newest or most popular posts from followed accounts
6. Listing the 25 most popular posts containing a given word (with optional AND operation for multiple words)


## Contributors
- Karel Kouambeng Fosso (karel.kouambengfosso@stud.hs-mannheim.de)
- Felix Mucha (felixjanmichael.mucha@stud.hs-mannheim.de)

## License
This project is licensed under the [GNU General Public License].

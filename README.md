# Big Data Engineering and Analytics Project 3
This repository contains the source code for an exercise in the field of Big Data Engineering and Analytics as part of the Master of Computer Science, SS2024.

## Instructions for Running the Project

To successfully run this project, please follow the steps outlined below:

1. Clone this repository to your local machine.
2. Navigate to the directory of the project.
2. Make sure that the Line Ends are configurated to be LF and not CRLF (down right corner in VS Code) safe each file with LF line endings
3. Build the Docker containers using the command `docker-compose build`.
4. Start the Docker containers with `docker-compose up`.
5. Access the web interface by visiting [http://localhost:5000](http://localhost:5000) and begin using the application.
6. To stop the application, press `Ctrl + C`.
7. Stop the Docker containers with `docker-compose dowm`.

Problems?

- Try to set the timeout to 60 seconds instead of 30 seconds.

### Checking Node Availability

If you need to verify the availability of the nodes, follow these steps:

1. Open a new terminal window.
2. Navigate to the directory containing the `docker-compose.yaml` file.
3. Access the shell of `cassandra_node1` by executing `docker-compose exec cassandra_node1 /bin/bash`.
4. Run the `nodetool status` command to check the status of the nodes.
5. Ensure that the status is `UN` (Up and Normal) for connectivity confirmation.

### Manual Database Interaction

For manual interaction with the databases:

- The `DB_tweet.py` script can be executed to interact with the Cassandra database.
- The `Graph_followers.py` script allows for interaction with the graph database.

Follow the instructions within each script for specific usage details.











# Old Readme



## Project Goal
This project implements a simple social network using NoSQL databases. Docker and Docker-Compose are used to deploy the database across two containers. The main functionalities of the system include storing and querying posts, follower relationships, and likes. The task includes several specific queries that the system should support


## Project Structure
- docker-compose.yml: Docker-Compose file to start up the NoSQL databases
- Dockerfile: Dockerfile for creating the database containers
- data_preprocessing.py: Script for preprocessing and loading data into the database
- queries.py: Python file with the required queries


## Technologies used
- Docker - Docker-compose
-  Python
-   oSql Database: Neo4J and Cassandra


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
   - Number of followers
   - Number of followed accounts
   - 25 newest or most popular posts from followed accounts
5. Listing the 25 most popular posts containing a given word (with optional AND operation for multiple words)


## Contributors
- Karel Kouambeng Fosso (karel.kouambengfosso@stud.hs-mannheim.de)
- Felix Mucha (felixjanmichael.mucha@stud.hs-mannheim.de)

## License
This project is licensed under the [GNU General Public License].
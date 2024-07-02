# Big Data Engineering and Analytics Project 3
This repository contains the source code for an exercise in the field of Big Data Engineering and Analytics as part of the Master of Computer Science, SS2024.

## Project Goal
This project implements a simple social network using NoSQL databases. Docker and Docker-Compose are used to deploy the database across two containers. The main functionalities of the system include storing and querying posts, follower relationships, and likes. The task includes several specific queries that the system should support

## Technologies Used
- FastApi
- Cassandra
- Neo4j
- Docker Compose

## Instructions for Running the Project
To successfully run this project, please follow the steps outlined below:

1. Clone this repository to your local machine.
2. Navigate to the directory of the project.
3. Ensure that line endings are set to LF (Unix-style) instead of CRLF (Windows-style). You can adjust this setting in the bottom-right corner of VS Code. Remember to save each file with LF line endings.
4. Build the Docker containers using the command `docker-compose build`.
5. Start the Docker containers with `docker-compose up`.
6. Access the web interface by visiting [http://localhost:5000](http://localhost:5000) and begin using the application.
7. To stop the application, press `Ctrl + C`.
8. Stop the Docker containers with `docker-compose dowm`.

Problems?

- Try to set the timeout to 60 seconds instead of 30 seconds in the docker-compose.yaml file.

## Checking Node Availability
If you need to verify the availability of the nodes, follow these steps:

1. Open a new terminal window.
2. Navigate to the directory containing the `docker-compose.yaml` file.
3. Access the shell of `cassandra_node1` by executing `docker-compose exec cassandra_node1 /bin/bash`.
4. Run the `nodetool status` command to check the status of the nodes.
5. Ensure that the status is `UN` (Up and Normal) for connectivity confirmation.

## Usage of the Api
- Execute the `status` command to determine which database is available.
- Import data into the Neo4j database using the `process_txt_file` command.
- Import data into the Cassandra database using the `import_tweets` command.
- Re-run the `status` command to verify that all data has been correctly loaded.

- Initialize the cache by accessing the `update_cache` endpoint.
- To view the likes table, initialize it using the `init_random_likes` endpoint.

## Manual Database Interaction
For manual interaction with the databases:

- The `DB_tweet.py` script can be executed to interact with the Cassandra database.
- The `Graph_followers.py` script allows for interaction with the graph database.

Follow the instructions within each script for specific usage details.

## Contributors
- Karel Kouambeng Fosso (karel.kouambengfosso@stud.hs-mannheim.de)
- Felix Mucha (felixjanmichael.mucha@stud.hs-mannheim.de)

## License
This project is licensed under the [GNU General Public License].
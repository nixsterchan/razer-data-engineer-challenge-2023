# Razer Data Engineer Technical Assignment: Part 1 Data Processing Program

## Overview
This project aims to create a docker image of a command line program that takes in arguments supplied by the user, and does a build using python. It aims to clean the data where cleaning procedures are modelled after the movies dataset.

## Prerequisites
- Docker
- Docker Compose

## Getting Started
1. Clone down this repository into a comfortable spot.
2. Change directory into the project repository.
3. Update any arguments within the docker-compose.yml file as needed.
4. Download the file from the link (https://datasets.imdbws.com/title.basics.tsv.gz) and place it in ./data/input folder.
5. Run ```docker-compose up --build```
6. Observe the results in the terminal.


## Customization of variables in docker compose
### Arguments
- input_mode    ->  change between the given modes (local, http, or api)
- input_file_path   ->  Path to the input file (local, http, or api)
- input_file_format   ->  Input file format. For now this supports tsv, csv and parquet
- output_mode   ->  change between the given modes (local, s3)
- output_file_path    ->  Name for the output file
- output_file_format    ->  Output file format. For now this supports tsv, csv and parquet

### Environment file
You can find this varibles in the template.env file. As this is just a template, make sure to supply your own .env file with the credentials filled up accordingly.

- AWS_ACCESS_KEY    ->  Set according to whichever AWS profile you want to upload data to.
- AWS_SECRET_KEY    ->  Set according to whichever AWS profile you want to upload data to.
- AWS_REGION_NAME   ->  Set according to whichever AWS profile you want to upload data to.

## Assumption made for data cleaning
- Movies dataset schema was used as reference for the cleaning process (https://datasets.imdbws.com/title.basics.tsv.gz)
- Schema:
    - 'tconst': 'str',
    - 'titleType': 'str',
    - 'primaryTitle': 'str',
    - 'originalTitle': 'str',
    - 'isAdult': 'bool',
    - 'startYear': 'int',
    - 'endYear': 'int',
    - 'runtimeMinutes': 'int',
    - 'genres': 'str'


# Essentials of Data Science EDS_Group3

## Installation

* python version 3.12.3 

make sure you are using the env:

python3 -m venv venv

## p1e.py

* to import all p1e.py files:

`python3 p1e.py -r`

* to import a specific file: 

`python3 p1e.py -d <dbname> <filename>`

### p1g.py tools:
This tool is to insert the data from './data/P1g/' fold into a database.

`p1g.py -d <dbname> <filename>`

* for example:

`p1g.py -d sqlite:///myhome.db ./data/P1g`

## smartthings.py

### Clean inserting with '-d'
This filters out non-important data (as defined in COLUMN_COMBINATIONS_TO_REMOVE).

* to import all smartthings files:

`python3 smartthings.py -d <db sqlite url> 'smartthings/smartthings.*'`

* to import a specific file (example):

`python3 smartthings.py -d <db sqlite url> 'smartthings/smartthings.202210.tsv.gz`

### Raw inserting with '--rawinsert'
Keeps all data and stores it in 1 _RAW table.

* to import all smartthings files:

`python3 smartthings.py --rawinsert <db sqlite url> 'smartthings/smartthings.*'`

* to import a specific file (example):

`python3 smartthings.py --rawinsert <db sqlite url> 'smartthings/smartthings.202210.tsv.gz`


## openweathermap.py


## database.py
Aim of this tool:
1. Initialize the database (create the tables) 
2. Fill the database with the data from the source files.



It should be described in the README.md file how to initialize the database and how to use all the tools to fill the database with the data from the source files.

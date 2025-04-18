# Essentials of Data Science EDS_Group3

## Contributions:

| Fullname        | Number   | GitHubName                                                      | File |
|-----------------|----------|-----------------------------------------------------------------|------|
| Wessel Porschen | s2004267 | [wesselporschen](https://github.com/wesselporschen)             | *    |
| Yufang Wang     | s3808394 | [Yufanggg](https://github.com/Yufanggg)                         | *    |
| Bas Ouwehand    | s0216445 | [bouwehand](https://github.com/bouwehand)                       | *    |
| Kylee Ornstein  | s4177789 | [kylo100](https://github.com/kylo100)            | *    |
| Agnes Czech     | s4069811 | [FallenApatheticAngel](https://github.com/FallenApatheticAngel) | *    |

*We request every team member to be graded equally for all of the project.

---

## Installation

* python version 3.12.3 

make sure you are using the env:

python3 -m venv venv

---

## CLI tools
There are 4 CLI tools which clean and insert the relevant data into the SQLite database (default: `sqlite:///myhome.db`).
The tools are:

* `p1e.py`: inserts `p1e` (electricity usage) data
* `p1g.py`: inserts `p1g` (gas usage) data
* `smartthings.py`: inserts `smartthings` (smart device) data
* `openweathermap.py`: Queries and inserts from OpenWeatherMap (Noordwijk, NL)
* `database.py`: Initialize the database (create the tables) 

**Important**: To easily execute the data analysis report notebooks, instead of running all tools separately, you can run the `init.sh` script which executes the tools in one go!

`chmod u+x init.sh`
`./init.sh`

If no argument is provides (like above), this will initialize a SQLite db at `sqlite:///myhome.db` (recommended to run data analysis report notebooks).

### p1e.py

* to import all p1e.py files:

`python p1e.py -r`

* to import a specific file: 

`python p1e.py -d <dbname> <filename>`

### p1g.py
This tool is to insert the data from './data/P1g/' fold into a database.

`python p1g.py -d <dbname> <filename>`

* for example:

`python p1g.py -d sqlite:///myhome.db ./data/P1g`

### smartthings.py

#### Clean inserting with '-d'
This filters out non-important data (as defined in COLUMN_COMBINATIONS_TO_REMOVE).

* to import all smartthings files:

`python smartthings.py -d <db sqlite url> 'smartthings/smartthings.*'`

* to import a specific file (example):

`python smartthings.py -d <db sqlite url> 'smartthings/smartthings.202210.tsv.gz`

#### Raw inserting with '--rawinsert'
Keeps all data and stores it in 1 _RAW table.

* to import all smartthings files:

`python smartthings.py --rawinsert <db sqlite url> 'smartthings/smartthings.*'`

* to import a specific file (example):

`python smartthings.py --rawinsert <db sqlite url> 'smartthings/smartthings.202210.tsv.gz`

### openweathermap.py

To store the weather data in the database

`python openweathermap.py -d sqlite:///myhome.db 2022-01-01 2024-04-01`

### database.py
Aim of this tool:
1. Initialize the database (create the tables) 
2. Fill the database with the data from the source files.

`python databse.py -d <dbname> <tablename>`

* for example:

`python databse.py -d sqlite:///myhome.db p1g`

---

## Data Analysis Reports
* report_electricity_usage.ipynb
* report_gas_weather.ipynb
* report_motion_analysis.ipynb
* report_temperature_difference.ipynb

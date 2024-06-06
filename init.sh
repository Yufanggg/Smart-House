#!/bin/bash


DB_URL=${1:-"sqlite:///myhome.db"}

echo -e "Starting database creation at: $DB_URL... (this may take a while)\n"
sleep 2

echo -e "Inserting openweathermap data (01-01-2022 - 01-04-2024)"
echo -e "-------------------------------------------------------\n"
sleep 1
python3 openweathermap.py -d $DB_URL 2022-01-01 2024-04-01

echo -e "Inserting P1e data"
echo -e "------------------\n"
python3 p1e.py -r

echo -e "Inserting P1g data"
echo -e "------------------\n"
python3 p1g.py -d $DB_URL ./data/P1g

echo -e "Clean inserting smartthings data."
echo -e "---------------------------------\n"
python3 smartthings.py -d "$DB_URL" "data/smartthings/smartthings.*"

echo -e "Raw inserting smartthings data."
echo -e "-------------------------------\n"
python3 smartthings.py --rawinsert "$DB_URL" "data/smartthings/smartthings.*"

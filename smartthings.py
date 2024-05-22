import sys
import gzip
import pandas as pd
import glob
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy import create_engine, inspect, Table, Column, Integer, Float, MetaData, String
from home_messages_db import HomeMessagesDB

TABLE_NAME = 'smartthings'
LOC_NAMES = ['garden', 'ground', 'floor', 'attic', 'kitchen', 'bathroom', 'living']

TABLE_COLUMNS = ['loc', 'level', 'name', 'time', 'capability', 'attribute', 'value', 'unit']
TABLE_TYPES = [String, String, String, Integer, String, String, String, String]

COLUMN_COMBINATIONS_TO_REMOVE = [
    ('bathroom (sink)', 'legendabsolute60149.switchAllOnOff1', 'switchAllOnOff'), # name, capability, attribute
    ('bathroom (bathtube)', 'legendabsolute60149.switchAllOnOff1', 'switchAllOnOff'),
    ('door (main)', 'signalStrength', 'lqi'),
    ('door (main)', 'signalStrength', 'rssi'),
    ('kitchen (table)', 'switchLevel', 'level'),
    ('door (garden)', 'signalStrength', 'lqi'),
    ('door (garden)', 'signalStrength', 'rssi'),
    ('door (garden)', 'voltageMeasurement', 'voltage'),
    ('kitchen (cam)', 'signalStrength', 'lqi'),
    ('kitchen (cam)', 'signalStrength', 'rssi'),
    ('kitchen (cam)', 'videoCapture', 'clip' ),
    ('kitchen (cam)', 'soundDetection', 'supportedSoundTypes'),
    ('kitchen (cam)', 'cameraPreset', 'presets'),
    ('kitchen (cam)', 'audioStream', 'uri'),
    ('kitchen (stairs)', 'powerSource', 'powerSource'),
    ('living room (tv)', 'samsungvd.supportsPowerOnByOcf', 'supportsPowerOnByOcf'),
    ('living room (wifi)', 'powerMeter', 'power'),
    ('living room (wifi)', 'energyMeter', 'energy'),
    ('living room (corner)', 'powerMeter', 'power'),
    ('living room (corner)', 'energyMeter', 'energy'),
    ('living room (corner)', 'switch', 'switch'),
    ('blue room (printer)', 'legendabsolute60149.signalMetrics', 'signalMetrics'),
    ('blue room (printer)', 'legendabsolute60149.switchAllOnOff1', 'switchAllOnOff'),
    ('green room (ceiling ii)', 'legendabsolute60149.switchAllOnOff1', 'switchAllOnOff'),
    ('green room (move cube)', 'winterdictionary35590.cube', 'action'),
    ('green room (move cube)', 'winterdictionary35590.cube', 'rotation'),
    ('green room (move cube)', 'winterdictionary35590.cube', 'face'),
    ('garden (above)', 'powerMeter', 'power'),
    ('garden (above)', 'energyMeter', 'energy'),
    ('garden (above)', 'switch', 'switch'),
    ('garden (ground)', 'switch', 'switch')
]

def matchByColCombinations(row):
    return any(all([row[col] == val for col, val in zip(['name', 'capability', 'attribute'], combination)]) for
 combination in COLUMN_COMBINATIONS_TO_REMOVE)

def main(db_url: str, filepath: str):

    # read tsv as dataframe
    df = pd.read_csv(filepath, compression='gzip', sep='\t')

    # data cleanup: drop any fully duplicate rows + fully NaN rows
    df.drop_duplicates(inplace=True)
    #df.dropna(inplace=True, how='all')

    # rename columns and make all 'name' entries lowercase
    df.rename(columns={'epoch': 'time'}, inplace=True)
    df['name'] = df['name'].str.lower()

    # filtering out 'non-important' data if required ('-d')
    if sys.argv[1] == '-d':
        print(f'Filtering out non-important data from {filepath}, this may take a while...')
        df = df[~df.apply(matchByColCombinations, axis=1)]

    # insert df into TABLE(s)
    if sys.argv[1] == '--rawinsert':
        print(f"Inserting {filepath} into {db_url} as {TABLE_NAME}_RAW...")
        MyDB.insert_df(df=df, table=TABLE_NAME+'_RAW', dtype = dict(zip(TABLE_COLUMNS, TABLE_TYPES)), if_exists='append', chunk_size=5000)
    
    if sys.argv[1] == '-d':
        print(f"Inserting {filepath} into {db_url} {TABLE_NAME} tables...")
        for LOC in LOC_NAMES:
            copy = df[df['loc'] == LOC]
            MyDB.insert_df(df=copy, table=TABLE_NAME+'_'+LOC, dtype = dict(zip(TABLE_COLUMNS, TABLE_TYPES)), if_exists='append', chunk_size=5000)


if __name__ == '__main__':
    sys.argv = [str(arg) for arg in sys.argv]

    if len(sys.argv) < 1:
        print('no options, please view -h')
        exit()

    if sys.argv[1] in ['-h', '--help']:
        print("""
Usage: smartthings.py [options]
Options:
    -h, --help          Show this help message

    -d DBURL insert into the project database (DBURL is a SQLAlchemy database URL)
       example:
       smartthings.py -d 'sqlite:///myhome.db' 'data/smartthings/smartthings.202210.tsv.gz'
       smartthings.py -d 'sqlite:///myhome.db' 'data/smartthings/smartthings.*' (WARNING: may take a while) 

    --rawinsert DBURL   Insert into database without filtering out 'nonimportant' data
        example:
        smartthings.py --rawinsert 'sqlite:///myhome.db' 'data/smartthings/smartthings.202210.tsv.gz'
        smartthings.py --rawinsert 'sqlite:///myhome.db' 'data/smartthings/smartthings.*' (WARNING: may take a while) 
            """)
        
    if sys.argv[1] in ['-d', '--rawinsert']:
        if not sys.argv[2] or not sys.argv[3]:
            print('invalid options, please view -h')
            exit()
        
        # initialise MyDB and create table if not exists in db
        # if table exists, will just print "Database already has table" and continue to main()
        MyDB = HomeMessagesDB(sys.argv[2])
        if sys.argv[1] == '--rawinsert':
            MyDB.create_table(name=TABLE_NAME+'_RAW', columns=[
                Column('loc', String(), nullable=True),
                Column('level', String(), nullable=True),
                Column('name', String(), nullable=True),
                Column('time', Integer(), nullable=True),
                Column('capability', String(), nullable=True),
                Column('attribute', String(), nullable=True),
                Column('value', String(), nullable=True),
                Column('unit', String(), nullable=True),
            ])

        if sys.argv[1] == '-d':
            for LOC in LOC_NAMES:
                MyDB.create_table(name=TABLE_NAME+'_'+LOC, columns= [
                    Column('loc', String(), nullable=True),
                    Column('level', String(), nullable=True),
                    Column('name', String(), nullable=True),
                    Column('time', Integer(), nullable=True),
                    Column('capability', String(), nullable=True),
                    Column('attribute', String(), nullable=True),
                    Column('value', String(), nullable=True),
                    Column('unit', String(), nullable=True),
                ])

        # main function (loading / cleaning / filtering (if '-d') / inserting of data into db)
        for file in glob.glob(sys.argv[3]):
            main(sys.argv[2], file)
        print("Done.")

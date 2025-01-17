WORK IN PROGRESS

Script should create schema and load data from file to desired table

in storage folder thre is samplecsv.csv

sequence:
python cli.py --operation create-schema
python cli.py --operation load-csv --table_name sample_table1 --file samplecsv.csv

works and populates desired table

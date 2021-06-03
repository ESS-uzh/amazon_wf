import psycopg2
import json
import pdb

with open('../../db_amazon_credentials.json', "r") as read_file:
    db = json.load(read_file)

def connect():
    return psycopg2.connect(user=db['user'], password=db['pwd'],
            database=db['database'], host=db['host'])

import os
from flask import Flask
from .database import Database
import logging
import json

# Logging conf
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('./amazon.log', mode="a")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

app = Flask(__name__)
SECRET_KEY = os.environ.get('SECRET_KEY')
if not SECRET_KEY:
    SECRET_KEY = 'rkgjngelkjrnelkejn'

app.config['SECRET_KEY'] = SECRET_KEY

with open('../db_amazon_credentials.json', "r") as read_file:
    db = json.load(read_file)

Database.initialise(user=db['user'], password=db['pwd'],
                    database=db['database'], host=db['host'])

from amazon_wf import routes

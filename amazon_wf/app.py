import json

from flask import request
from flask import Flask, render_template

from amazon_wf.database import Database
from amazon_wf.tile import Tile
from amazon_wf.biodivmap import Biodivmap
from amazon_wf.database import CursorFromConnectionPool

import pdb

app = Flask(__name__)


with open('../../db_amazon_credentials.json', "r") as read_file:
    db = json.load(read_file)

Database.initialise(user=db['user'], password=db['pwd'], 
                    database=db['database'], host=db['host'])


def display_results(batch):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT id, name, level, acquisition_date, size_mb, status, available, proc_status FROM tiles FULL JOIN biodivmap ON tiles.id = biodivmap.tile_id WHERE tiles.tile_loc=%s;''', (batch,))
        data = cursor.fetchall()
        return data


@app.route('/')
def homepage():
    return render_template('home.html')

@app.route('/results')
def results():
    batch = request.args.get('batch')
    print(batch)
    data = display_results(batch)
    print(data)
    #pdb.set_trace()
    #return render_template('tile.html', tile=tile)



app.run(host='0.0.0.0', port=4995, debug=True)

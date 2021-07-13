import json

from flask import request
from flask import Flask, render_template, flash, redirect, url_for

from amazon_wf.database import Database
from amazon_wf.tile import Tile
from amazon_wf.location import Location
from amazon_wf.biodivmap import Biodivmap
from amazon_wf.user import User
from amazon_wf.database import CursorFromConnectionPool

import pdb

app = Flask(__name__)


with open('../../db_amazon_credentials.json', "r") as read_file:
    db = json.load(read_file)

Database.initialise(user=db['user'], password=db['pwd'],
                    database=db['database'], host=db['host'])

USERS = User.get_users()
BATCHES = [i for i in range(1, 18)]

def display_results(batch):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT tiles.id, tiles.name, tiles.level,
                tiles.acquisition_date, tiles.cloud_coverage, tiles.status,
                tiles.available, biodivmap.proc_status, users.name
                FROM tiles
                FULL JOIN biodivmap
                ON tiles.id = biodivmap.tile_id
                FULL JOIN users
                ON users.id = tiles.user_id
                WHERE tiles.tile_loc=%s;''', (batch,))
        data = cursor.fetchall()
        return data


def display_availables(batch, proc_status):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT tiles.id, tiles.name, tiles.level,
                tiles.acquisition_date, tiles.size_mb, tiles.status,
                tiles.available, biodivmap.proc_status, users.name
                FROM tiles
                FULL JOIN biodivmap
                ON tiles.id = biodivmap.tile_id
                FULL JOIN users
                ON users.id = tiles.user_id
                WHERE tiles.tile_loc=%s and
                biodivmap.proc_status=%s and
                tiles.user_id is NULL;''', (batch, proc_status))
        data = cursor.fetchall()
        return data


def display_update(user_id):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT tiles.id, tiles.name,
                biodivmap.proc_status, users.name
                FROM tiles
                LEFT JOIN biodivmap
                ON tiles.id = biodivmap.tile_id
                LEFT JOIN users
                ON tiles.user_id = users.id
                WHERE biodivmap.proc_status='pca' and
                tiles.user_id=%s;''', (user_id,))
        data = cursor.fetchall()
        return data

def display_discard(user_id):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT tiles.id, tiles.name,
                biodivmap.proc_status, users.name
                FROM tiles
                LEFT JOIN biodivmap
                ON tiles.id = biodivmap.tile_id
                LEFT JOIN users
                ON tiles.user_id = users.id
                WHERE (biodivmap.proc_status='pca' or
                biodivmap.proc_status='pca_ready') and
                tiles.user_id=%s;''', (user_id,))
        data = cursor.fetchall()
        return data

@app.route('/results/<batch>', methods=['GET'])
def results(batch):
    dirpath_tiles = Location.get_dirpath_from_loc(batch)
    data = display_results(batch)
    return render_template('result.html',
                           batch=batch,
                           data=data,
                           dirpath_tiles=dirpath_tiles,
                           batches=BATCHES,
                           users=USERS)


@app.route('/availables/<batch>', methods=['GET', 'POST'])
def availables(batch):
    dirpath_tiles = Location.get_dirpath_from_loc(batch)
    data = display_availables(batch, proc_status='pca')
    print(batch)
    if request.method == 'POST':
        print(batch)
        user_name = request.form.get('user')
        print(user_name)
        tiles_name = request.form.getlist('tile')
        print(tiles_name)
        if user_name and tiles_name:
            user_db = User.load_by_name(user_name)
            for tile_name in tiles_name:
                tile_db = Tile.load_by_tile_name(tile_name)
                if tile_db.user_id:
                    flash('Tiles already taken! Please try again.', 'danger')
                    return url_for('availables', batch=batch)
                else:
                    tile_db.update_tile_user_id(user_db._id)
            flash('Tiles selected for batch {}'.format(batch), 'success')
            return redirect(url_for('results', batch=batch))
        else:
            flash('Please select user and tile/s!', 'danger')

    return render_template('available.html',
                        batch=batch,
                        data=data,
                        dirpath_tiles=dirpath_tiles,
                        batches=BATCHES,
                        users=USERS)


@app.route('/update/<user_name>', methods=['GET', 'POST'])
def update(user_name):
    user = User.load_by_name(user_name)
    if request.method == 'POST':
        tile_name = request.form.get('tile')
        if not tile_name:
            flash('Please select a tile!', 'danger')
            return redirect(url_for('update', user_name=user_name))
        tile_db = Tile.load_by_tile_name(tile_name)
        tile_id = tile_db.get_tile_id()
        bio_db = Biodivmap.load_by_tile_id(tile_id)
        batch=tile_db.tile_loc
        if request.form.get('action') == 'update':
            bio_db.update_proc_status('pca_ready')
            flash('Tile {} proc_level updated!'.format(tile_name), 'success')
        elif request.form.get('action') == 'discard':
            print('discard')
            if bio_db.proc_status == 'pca':
                tile_db.update_tile_user_id_as_null()
            elif bio_db.proc_status == 'pca_ready':
                bio_db.update_proc_status('pca')
            flash('Tile: {} discarded!'.format(tile_name), 'success')
        return redirect(url_for('results', batch=batch))
    data_to_update = display_update(user._id)
    print(data_to_update)
    data_to_discard = display_discard(user._id)
    print(data_to_discard)
    return render_template('update.html',
                           user_name=user_name,
                           data_update=data_to_update,
                           data_discard=data_to_discard,
                           batches=BATCHES,
                           users=USERS)

if __name__ == '__main__':
    # Quick test configuration. Please use proper Flask configuration options
    # in production settings, and use a separate file or environment variables
    # to manage the secret key!
    app.secret_key = 'mykey'
    app.config['SECRET_KEY'] = 'mykey'

    app.run(host='0.0.0.0', port=4995, debug=True)

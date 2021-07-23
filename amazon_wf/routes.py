import json
import geopandas as gpd
import folium
from shapely import wkb

from flask import request
from flask import Flask, render_template, flash, redirect, url_for, session
from werkzeug.security import check_password_hash, generate_password_hash
from amazon_wf import app

from amazon_wf.tile import Tile
from amazon_wf.location import Location
from amazon_wf.biodivmap import Biodivmap
from amazon_wf.user import User
from amazon_wf.database import CursorFromConnectionPool

import pdb

USERS = User.get_users()
BATCHES = [i for i in range(1, 18)]


def display_formap(batch):
    with CursorFromConnectionPool() as cursor:
        cursor.execute('''SELECT tiles.name,
                tiles.acquisition_date, tiles.cloud_coverage, tiles.status,
                tiles.available, tiles.footprint
                FROM tiles WHERE tiles.tile_loc=%s;''', (batch,))
        data = cursor.fetchall()
        return data


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


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_name = request.form.get('user_name')
        print(user_name)
        user_password = request.form.get('user_password')
        if user_name and user_password:
            user_db = User.load_by_name(user_name)
            print(user_db._id)
            error= None
            if not check_password_hash(user_db.pwd, user_password):
                error = 'Incorrect password.'
            if error is None:
                session.clear()
                session['user_id'] = user_db._id
                flash(f'Welcome {user_name}!', 'success')
                return redirect(url_for('results', batch=1))
            flash(error, 'danger')
        else:
            flash('Please select a user and insert a password', 'danger')

    return render_template('login.html',
                           users=USERS)
@app.route('/logout')
def logout():
    session.clear()
    flash(f'You have successfully logged out!', 'success')
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        user_name = request.form.get('user_name')
        old_pwd = request.form.get('old_password')
        new_pwd = request.form.get('new_password')
        retype_pwd = request.form.get('retype_new_password')
        if all(v is not None for v in [user_name, old_pwd, new_pwd, retype_pwd]):
            user_db = User.load_by_name(user_name)
            print(user_db._id)
            error= None
            if not check_password_hash(user_db.pwd, old_pwd):
                error = 'Incorrect old password!'
            if not new_pwd == retype_pwd:
                error = 'Passwords must match!'
                print('no match')
            if error is None:
                pwd_hash = generate_password_hash(new_pwd)
                user_db.pwd = pwd_hash
                user_db.update_user()
                flash(f'Password successfully updated for: {user_name}!', 'success')
                return redirect(url_for('login'))
            flash(error, 'danger')
        else:
            flash('Please select a user and fill in all fields', 'danger')
    return render_template('register.html',
                           users=USERS)

@app.route('/maps/<batch>', methods=['GET'])
def maps(batch):
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))

    user_db = User.load_by_id(user_id)
    dirpath_tiles = Location.get_dirpath_from_loc(batch)
    data = display_formap(batch)

    df = gpd.GeoDataFrame(data, columns =['Name', 'Date', 'CloudC', 'Status', 'Available', 'Geometry'])

    m = folium.Map(location=[-3.46, -62.21], zoom_start=5, tiles='Stamen Terrain')

    for _, r in df.iterrows():
    # Without simplifying the representation of each borough,
    # the map might not be displayed
        if not r['Geometry']:
            print(r['Name'])
        else:
            sim_geo = gpd.GeoSeries(wkb.loads(bytes.fromhex(r['Geometry']))).simplify(tolerance=0.001)
            geo_j = sim_geo.to_json()
            geo_j = folium.GeoJson(data=geo_j,
                               style_function=lambda x: {'fillColor': 'orange'})
            strg = f"{r['Name']}, {r['Status']}"
            folium.Popup(strg).add_to(geo_j)
            geo_j.add_to(m)

    return render_template('map.html',
                           maps = m._repr_html_(),
                           batch=batch,
                           data=data,
                           user_name=user_db.name,
                           dirpath_tiles=dirpath_tiles,
                           batches=BATCHES,
                           users=USERS)


@app.route('/results/<batch>', methods=['GET'])
def results(batch):
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))
    user_db = User.load_by_id(user_id)
    dirpath_tiles = Location.get_dirpath_from_loc(batch)
    data = display_results(batch)
    # sanitize data by replacing None with an empty string
    data = [(tuple(x if x else '' for x in _ )) for _ in data]
    return render_template('result.html',
                           batch=batch,
                           data=data,
                           user_name=user_db.name,
                           dirpath_tiles=dirpath_tiles,
                           batches=BATCHES,
                           users=USERS)


@app.route('/availables/<batch>', methods=['GET', 'POST'])
def availables(batch):
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))
    user_db = User.load_by_id(user_id)
    dirpath_tiles = Location.get_dirpath_from_loc(batch)
    data = display_availables(batch, proc_status='pca')
    if request.method == 'POST':
        print(batch)
        tiles_name = request.form.getlist('tile')
        print(tiles_name)
        if tiles_name:
            for tile_name in tiles_name:
                tile_db = Tile.load_by_tile_name(tile_name)
                if tile_db.user_id:
                    flash('Tiles already taken! Please try again.', 'danger')
                    return redirect(url_for('availables', batch=batch))
                else:
                    tile_db.update_tile_user_id(user_db._id)
            flash('Tiles selected for batch {}'.format(batch), 'success')
            return redirect(url_for('results', batch=batch))
        else:
            flash('Please select tile/s!', 'danger')

    return render_template('available.html',
                        batch=batch,
                        data=data,
                        user_name=user_db.name,
                        dirpath_tiles=dirpath_tiles,
                        batches=BATCHES)


@app.route('/update/<user_name>', methods=['GET', 'POST'])
def update(user_name):
    user_id = session.get('user_id')
    if not user_id:
        return redirect(url_for('login'))
    user_db = User.load_by_id(user_id)
    if request.method == 'POST':
        tile_name = request.form.get('tile')
        if not tile_name:
            flash('Please select a tile!', 'danger')
            return redirect(url_for('update', user_name=user_db.name))
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
    data_to_update = display_update(user_db._id)
    print(data_to_update)
    data_to_discard = display_discard(user_db._id)
    print(data_to_discard)
    return render_template('update.html',
                           user_name=user_db.name,
                           data_update=data_to_update,
                           data_discard=data_to_discard,
                           batches=BATCHES,
                           users=USERS)


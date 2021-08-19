import os
import pathlib
import fnmatch
import time
import logging
import zipfile
import multiprocessing as mp
import subprocess
import sentinelsat
from sentinelsat import SentinelAPI

from datetime import date
from dateutil.relativedelta import relativedelta
import sentinelsat

import geoRpro.utils as ut

from amazon_wf.tile import Tile
from amazon_wf.location import Location
from amazon_wf.biodivmap import Biodivmap
from amazon_wf.compute import get_dirs, pre_process, gen_R_script

import pdb


logger = logging.getLogger(__name__)

def size_in_mb(strg_size):
    """
    Helper
    """
    num, dim = strg_size.split()
    if dim == 'GB':
        return round(float(num)*1000, 1)
    return round(float(num), 1)


def uuid_with_larger_size(products):
    """
    Helper
    """
    lowest_size = 0.0
    uuid = None
    for k in products.keys():
        if size_in_mb(products.get(k)['size']) > lowest_size:
            lowest_size = size_in_mb(products.get(k)['size'])
            uuid = products.get(k)['uuid']
    return uuid


def update_db_for_batch(api, tile_loc, level='2A', date=('20200620', '20200820'), cc=(0, 1)):
    logger.info(f'Populate db for batch: {tile_loc}')
    logger.info(f'Level selected: {level}')
    logger.info(f'Date selected: {date}')
    # select tiles with status and uuid == NULL
    ids = Tile.get_tiles_no_uuid(tile_loc)
    if ids:
        logger.info(f'Found: {len(ids)} tiles incomplete')
        for _id in ids:
            time.sleep(2)
            tile_db = Tile.load_by_tile_id(_id)
            logger.info(f'Trying: {tile_db.name}')
            products = api.query(filename=f'*_{tile_db.name}_*',
                     date=date,
                     platformname='Sentinel-2',
                     processingLevel=f'Level-{level}',
                     cloudcoverpercentage=cc)

            if len(products) == 0:
                logger.info(f'No match found for: {tile_db.name}')
                continue

            uuid = uuid_with_larger_size(products)

            begin_acquisition_date = products.get(uuid)['beginposition'].date()
            tile_db.date = begin_acquisition_date.strftime('%Y-%m-%d')
            tile_db.level = products.get(uuid)['processinglevel'].split('-')[1]
            tile_db.cc = round(products.get(uuid)['cloudcoverpercentage'], 4)
            tile_db.size_mb = size_in_mb(products.get(uuid)['size'])
            tile_db.uuid = products.get(uuid)['uuid']
            tile_db.geometry = products.get(tile_db.uuid)['footprint']
            tile_db.fname = products.get(tile_db.uuid)['filename']
            tile_db.update_tile()
            logger.info(f'Tile: {tile_db.name} updated')
    else:
        logger.info(f'No incomplete tiles found for batch: {tile_loc}')
    return 0


def download_for_batch(api, tile_loc, basedir, status=None):
    """
    Download tiles of batch tile_loc and update database
    """
    if not status:
        to_download = Tile.get_downloadable(tile_loc)
    elif status == 'corrupted':
        to_download = Tile.get_downloadable_with_status(tile_loc, 'corrupted')

    if to_download:
        logger.info(f'Found: {len(to_download)} tiles to download')
        location_db = Location.load_by_loc(tile_loc)
        dirpath = os.path.join(basedir, location_db.dirpath)
        dirpath = pathlib.Path(dirpath)
        if not dirpath.is_dir():
            dirpath.mkdir(parents=True, exist_ok=True)
        logger.info(f'Location: {dirpath}')
        for uuid in to_download:
            time.sleep(5)
            tile_db = Tile.load_by_tile_uuid(uuid)
            fzip = '.'.join([tile_db.fname.split('.')[0], 'zip'])
            fzippath = os.path.join(dirpath, fzip)
            logger.info(f'Trying tile name: {tile_db.name}, uuid: {tile_db.uuid}')
            try:
                products = api.download(uuid, dirpath)
                logger.info(f'{tile_db.name} downloaded')
            except sentinelsat.exceptions.LTATriggered:
                logger.info(f'{uuid} not online, skipped..')
                continue
            except sentinelsat.exceptions.ServerError:
                logger.info(f'Got server error..')
                return 2
            try:
                logger.info(f'Try to unzip...')
                with zipfile.ZipFile(fzippath, 'r') as zip_ref:
                    zip_ref.extractall(dirpath)
            except zipfile.BadZipFile:
                tile_db.update_tile_status('corrupted')
                logger.info(f'update {tile_db.name} as corrupted')
                os.remove(fzippath)
                continue

            os.remove(fzippath)
            tile_db.update_tile_availability(True)
            if tile_db.level == '2A':
                tile_db.update_tile_status('ready')
                logger.info(f'update {tile_db.name} as available and ready')
            elif tile_db.level == '1C':
                tile_db.update_tile_status('downloaded')
                logger.info(f'update {tile_db.name} as available and downloaded')
        return 1
    else:
        logger.info(f'No tiles found to be downloaded for batch: {tile_loc}')
    return 0

def correct_1c_to_2a_for_batch(tile_loc, basedir):
    # gather tiles with status downloaded
    tiles_1c = Tile.get_tiles_id_with_status(tile_loc, 'downloaded')
    if tiles_1c:
        logger.info(f'Found: {len(tiles_1c)} tiles 1C to correct')

        # this works only when run on the dcorr server
        my_env = os.environ.copy()
        my_env['PATH'] = '/home/ubuntu/Sen2Cor-02.08.00-Linux64/bin:' + my_env['PATH']
        my_env['SEN2COR_HOME'] = '/home/ubuntu/sen2cor/2.8'
        my_env['SEN2COR_BIN'] = '/home/ubuntu/Sen2Cor-02.08.00-Linux64/lib/python2.7/site-packages/sen2cor'
        my_env['LC_NUMERIC'] = 'C'
        my_env['GDAL_DATA'] = '/home/ubuntu/Sen2Cor-02.08.00-Linux64/share/gdal'
        my_env['GDAL_DRIVER_PATH']= 'disable'

        location_db = Location.load_by_loc(tile_loc)
        for _id in tiles_1c:
            # load a tile from id
            tile_db = Tile.load_by_tile_id(_id)
            logger.info(f'Start correction for tile {tile_db.name}')
            # get the full path to tile
            full_basedir = os.path.join(basedir, location_db.dirpath)
            fpath = os.path.join(full_basedir, tile_db.fname)

            cmd = f'L2A_Process {fpath}',

            try:
                result = subprocess.check_output(cmd, env=my_env, shell=True)
            except subprocess.CalledProcessError as e:
                logger.error(e.output)
                continue

            logger.info(f'Finished correction for tile {tile_db.name}')
            tile_db.level = '1C, 2A'

            new_fname = [_dir for _dir in os.listdir(full_basedir) if
                fnmatch.fnmatch(_dir, f'*_MSIL2A_*_{tile_db.name}_*')][0]

            tile_db.fname = new_fname
            tile_db.status = 'ready'
            tile_db.update_tile()
            logger.info(f'Updated db for tile {tile_db.name}')
    else:
        logger.info(f'No tiles found to be corrected for batch: {tile_loc}')
    return 0


def stack_for_batch(tile_loc, basedir,
        dirpath = 'ess_biodiversity/data/processed_data/amazon/stacks'):
    tiles_id_ready = Tile.get_tiles_id_with_status(tile_loc, 'ready')
    tiles_id_processed = Biodivmap.get_tiles_id_with_any_proc_status(tiles_id_ready)
    tiles_id_to_be_processed = [_id for _id in tiles_id_ready if _id
            not in tiles_id_processed]


    if tiles_id_to_be_processed:
        logger.info(f'Found: {len(tiles_id_to_be_processed)} tiles to process')
        dirpath = os.path.join(dirpath, f'batch{tile_loc:03}')
        if not Location.get_loc_from_dirpath(dirpath):
            # create raster_loc
            location_db = Location(dirpath)
            location_db.save_to_db()
            logger.info(f'Created new loc for batch: {tile_loc}')

        loc = Location.get_loc_from_dirpath(dirpath)[0]
        logger.info(f'Location: {loc}')

        indir = Location.get_dirpath_from_loc(tile_loc)
        indir = os.path.join(basedir, indir)
        outdir = os.path.join(basedir, dirpath)
        outdir = pathlib.Path(outdir)
        if not outdir.is_dir():
            outdir.mkdir(parents=True, exist_ok=True)
        logger.info(f'Location: {outdir}')

        hdr2a_fp = os.path.join(os.path.dirname(indir), 'template_sent2A.hdr')
        hdr2b_fp = os.path.join(os.path.dirname(indir), 'template_sent2B.hdr')

        bands = ['B02_10m', 'B03_10m', 'B04_10m', 'B05_20m', 'B06_20m',
        'B07_20m', 'B08_10m', 'B8A_20m', 'B11_20m', 'B12_20m']

        for subset in ut.gen_sublist(tiles_id_to_be_processed, 2):
            # Multiprocess runs each dataset as separate process. A part from the
            # performance boost of running dataset in parallel this step is necessary
            # in order to return the memory to the system after each process ends.

            tiles_db = [Tile.load_by_tile_id(t_id) for t_id in subset]
            data = get_dirs(indir, [t.fname for t in tiles_db])

            jobs = []
            for _dir in data:
                p = mp.Process(target=pre_process, args=(_dir, bands,
                    outdir, hdr2a_fp, hdr2b_fp))
                p.start()
                jobs.append(p)

            for j in jobs:
                j.join()

            # insert entries in biodivmap db

            for t_id in subset:
                biodivmap_db = Biodivmap(t_id, raster_loc=loc, proc_status='raster')
                biodivmap_db.save_to_db()
                logger.info(f'Inserted {t_id} into db with proc_status=raster')
        return 1
    logger.info(f'No tiles found to be pre-processed for batch: {tile_loc}')
    return 0


def biodivmap_pca_for_batch(tile_loc, basedir, proc_status='raster',
    dirpath = 'ess_biodiversity/data/processed_data/amazon/biodivmap'):

    tiles_id_ready = Tile.get_tiles_id_with_status(tile_loc, 'ready')
    procs_and_tiles_id_raster = Biodivmap.get_procs_and_tiles_id_with_proc_status(tiles_id_ready, proc_status)

    if procs_and_tiles_id_raster:
        dirpath = os.path.join(dirpath, f'batch{tile_loc:03}')
        logger.info(f'Found: {len(procs_and_tiles_id_raster)} rasters to process')
        if not Location.get_loc_from_dirpath(dirpath):
            # create raster_loc
            location_db = Location(dirpath)
            location_db.save_to_db()
            logger.info(f'Created new loc for batch: {tile_loc}')

        loc = Location.get_loc_from_dirpath(dirpath)[0]
        logger.info(f'Location: {loc}')

        biodivmap_db = Biodivmap.load_by_proc_id(procs_and_tiles_id_raster[0][0])
        indir = Location.get_dirpath_from_loc(biodivmap_db.raster_loc)
        indir = os.path.join(basedir, indir)
        outdir = os.path.join(basedir, dirpath)
        outdir = pathlib.Path(outdir)
        if not outdir.is_dir():
            outdir.mkdir(parents=True, exist_ok=True)
        logger.info(f'Location: {outdir}')

        template_pca = os.path.join(os.path.dirname(outdir), 'amazon_template_pca.R')

        for proc_id, tile_id in procs_and_tiles_id_raster:
            logger.info(f'PCA processing for tile: {tile_id}')
            mapping = {}
            biodivmap_db = Biodivmap.load_by_proc_id(proc_id)
            if not biodivmap_db.out_loc:
                biodivmap_db.update_out_loc(loc)
                logger.info(f'Update out_loc location to {loc}')
            tile_db = Tile.load_by_tile_id(tile_id)
            fname_raster = '_'.join([tile_db.name, tile_db.date.strftime('%Y%m%d')])+'.tif'
            fname_rscript = fname_raster.replace('.tif', '.R')
            mapping['path_to_raster'] = os.path.join(indir, fname_raster)
            mapping['path_to_out'] = outdir
            rscript_path = gen_R_script(template_pca, mapping, fname_rscript)
            logger.info(f'Generated R script for tile: {tile_id}')
            try:
                result = subprocess.check_output(["Rscript", rscript_path])
            except subprocess.CalledProcessError as e:
                logger.error(e.output)
                os.remove(rscript_path)
                biodivmap_db.update_proc_status('pca_error')
                continue
            # delete the rscript_path
            os.remove(rscript_path)
            biodivmap_db.update_proc_status('pca')
    logger.info(f'No tiles found to be used for pca for batch: {tile_loc}')
    return 0

def biodivmap_out_for_batch(tile_loc, basedir, proc_status='pca_ready',
    dirpath = 'ess_biodiversity/data/processed_data/amazon/biodivmap'):

    tiles_id_pca_ready = Tile.get_tiles_id_with_status(tile_loc, 'ready')
    procs_and_tiles_id_raster = Biodivmap.get_procs_and_tiles_id_with_proc_status(tiles_id_pca_ready, proc_status)

    if procs_and_tiles_id_raster:
        dirpath = os.path.join(dirpath, f'batch{tile_loc:03}')
        logger.info(f'Found: {len(procs_and_tiles_id_raster)} rasters to process')

        # Location must exist
        loc = Location.get_loc_from_dirpath(dirpath)[0]
        logger.info(f'Location: {loc}')

        biodivmap_db = Biodivmap.load_by_proc_id(procs_and_tiles_id_raster[0][0])
        stack_dir = Location.get_dirpath_from_loc(biodivmap_db.raster_loc)
        stack_dir = os.path.join(basedir, stack_dir)
        biov_dir = os.path.join(basedir, dirpath)
        #outdir = pathlib.Path(outdir)

        template_out = os.path.join(os.path.dirname(biov_dir), 'amazon_template_out.R')

        for proc_id, tile_id in procs_and_tiles_id_raster:
            logger.info(f'Biodivmap processing processing for tile: {tile_id}')
            mapping = {}
            biodivmap_db = Biodivmap.load_by_proc_id(proc_id)
            tile_db = Tile.load_by_tile_id(tile_id)
            fname_raster = '_'.join([tile_db.name, tile_db.date.strftime('%Y%m%d')])+'.tif'
            fname_rscript = fname_raster.replace('.tif', '.R')
            rname = fname_raster.replace('.tif', '')
            mapping['stack_dir'] = stack_dir
            mapping['stack_name'] = rname
            # too be changed
            mapping['output_dir'] = biov_dir
            rscript_path = gen_R_script(template_out, mapping, fname_rscript)
            logger.info(f'Generated R script for tile: {tile_id}')
            try:
                result = subprocess.check_output(["Rscript", rscript_path])
            except subprocess.CalledProcessError as e:
                logger.error(e.output)
                os.remove(rscript_path)
                biodivmap_db.update_proc_status('out_error')
                continue
            # delete the rscript_path
            os.remove(rscript_path)
            biodivmap_db.update_proc_status('out')
    logger.info(f'No tiles found to be used for biodivmap for batch: {tile_loc}')
    return 0

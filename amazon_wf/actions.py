import os
import pathlib
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

def download_for_batch(api, tile_loc, basedir):
    """
    Download tiles of batch tile_loc and update database
    """
    to_download = Tile.get_downloadable(tile_loc)
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
            tile_db.update_tile_status('ready')
            logger.info(f'update {tile_db.name} as available and ready')
        return 1
    logger.info(f'No tiles found to be downloaded for batch: {tile_loc}')
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



def biodivmap_pca_for_batch(tile_loc, basedir,
    dirpath = 'ess_biodiversity/data/processed_data/amazon/biodivmap'):

    tiles_id_ready = Tile.get_tiles_id_with_status(tile_loc, 'ready')
    procs_and_tiles_id_raster = Biodivmap.get_procs_and_tiles_id_with_proc_status(tiles_id_ready, 'raster')

    if procs_and_tiles_id_raster:
        dirpath = os.path.join(dirpath, f'batch{tile_loc:03}')
        logger.info(f'Found: {len(procs_and_tiles_id_raster)} raster to process')
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
                biodivmap_db.update_proc_status('error pca')
                continue
            # delete the rscript_path
            os.remove(rscript_path)
            biodivmap_db.update_proc_status('pca')

import os
import pathlib
import time
import logging
import zipfile
import sentinelsat
from sentinelsat import SentinelAPI

from datetime import date
from dateutil.relativedelta import relativedelta

import sentinelsat

from amazon_wf.tile import Tile
from amazon_wf.location import Location

import pdb


logger = logging.getLogger(__name__)


def download_batch(api, tile_loc, basedir):
    """
    Download tiles of batch tile_loc and update database 
    """
    logger.info(f'Download batch: {tile_loc}')
    to_download = Tile.get_downloadable(tile_loc)
    if to_download:
        logger.info(f'Found: {len(to_download)} tiles to download')
        location_db = Location.load_by_loc(tile_loc)
        dirpath = os.path.join(basedir, '/'.join(location_db.dirpath.split('/')[1:]))
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
    return 0

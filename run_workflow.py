import os
import pathlib
import time
import logging
import json
import zipfile
import sentinelsat
from sentinelsat import SentinelAPI
import pandas as pd

from datetime import date
from dateutil.relativedelta import relativedelta

from sentinelsat import SentinelAPI

from amazon_wf.actions import search_tiles_for_batch, search_and_update_tile, download_for_batch, stack_for_batch, biodivmap_pca_for_batch, correct_1c_to_2a_for_batch, biodivmap_out_for_batch, biodivmap_beta_for_batch, get_product, update_db_for_tile, create_new_tile, update_db_for_tile_uuid

import pdb


logger = logging.getLogger(__name__)

def update_db(**kwargs):
    # connect to the API
    # A .netrc file with username and password must be present in the home folder
    #api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
    logger.info('Starting update db procedure..')
    update_db_for_batch(api, **kwargs)


def download(trials=3, **kwargs):
    for i in range(1, trials+1):
        logger.info('Starting download procedure..')
        logger.info(f'Run: {i}')
        # connect to the API
        # A .netrc file with username and password must be present in the home folder
        api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
        download_status = download_for_batch(api, **kwargs)
        if download_status == 1 and i < (trials+1):
            time.sleep(120)
            continue
        elif download_status == 2 and i < (trials+1):
            logger.info('Workflow halted')
            time.sleep(60)
            continue
        elif download_status == 0:
            break
    logger.info(f'Download exited with status: {download_status}')

def correction(**kwargs):
    logger.info('Starting atmospheric correction procedure..')
    correct_1c_to_2a_for_batch(**kwargs)


def stack():
    logger.info('Starting pre-processing procedure..')
    stack_for_batch(BATCH, BASEDIR)


def pca(**kwargs):
    logger.info('Starting PCA procedure..')
    biodivmap_pca_for_batch(**kwargs)

def out(**kwargs):
    logger.info('Starting Biodivmap procedure..')
    biodivmap_out_for_batch(**kwargs)

def beta(**kwargs):
    logger.info('Starting Biodivmap procedure..')
    biodivmap_beta_for_batch(**kwargs)

if __name__ == "__main__":


    BASEDIR = '/home/ubuntu/mnt/shared/group'
    BATCH = 5  # would be 18

    logger.info(f'Starting workflow for batch: {BATCH}')
    
    api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
    #search_tiles_for_batch(api, BATCH, date=('20190120', '20221020'), cc=(0, 8))
    #update_db(tile_loc=BATCH, level='2A', date=('20200420', '20200820'), cc=(0, 5))
    #products = get_product(api, tile_name='T20MLC', level='2A', date=('20190120', '20230501'), cc=(0, 4), verbose=True)
    import pdb
    pdb.set_trace()
    #print()
    #update_db_for_tile('T20MLC', products['c8ac580a-2de6-4595-bc73-26e6988f8da8'], status='to_download')
    
    #create_new_tile('T19MFT', 3, 'cf397141-d4c7-4a36-86b1-f6d2394dcca9')
    #update_db_for_tile_uuid('a1d4512e-2dfc-4d93-b1c6-a77285e7b139', products['a1d4512e-2dfc-4d93-b1c6-a77285e7b139'], 'to_download')
    #update_db_for_tile_uuid('07db8913-c32f-4528-ad58-106bae4a9a20', products['07db8913-c32f-4528-ad58-106bae4a9a20'], 'to_download')
    
    download_for_batch(api, tile_loc=BATCH, basedir=BASEDIR, status='to_download')
    #correction(tile_loc=BATCH, basedir=BASEDIR)
    #stack()
    #pca(tile_loc=BATCH, basedir=BASEDIR, proc_status='raster')
    #search_and_update_tile(api, "T20MNE")
    #out(tile_loc=BATCH, basedir=BASEDIR, proc_status="pca_ready")
    #beta(tile_loc=BATCH, basedir=BASEDIR)
    # c8ac580a-2de6-4595-bc73-26e6988f8da8

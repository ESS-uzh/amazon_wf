import os
import pathlib
import time
import logging
import json
import zipfile
import sentinelsat
from sentinelsat import SentinelAPI

from datetime import date
from dateutil.relativedelta import relativedelta

from sentinelsat import SentinelAPI

from amazon_wf.actions import update_db_for_batch
from amazon_wf.actions import download_for_batch
from amazon_wf.actions import stack_for_batch
from amazon_wf.actions import biodivmap_pca_for_batch
from amazon_wf.actions import correct_1c_to_2a_for_batch
from amazon_wf.database import Database

import pdb


logger = logging.getLogger(__name__)

def update_db(**kwargs):
    # connect to the API
    # A .netrc file with username and password must be present in the home folder
    api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
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
            time.sleep(180)
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

if __name__ == "__main__":

    with open('../../db_amazon_credentials.json', "r") as read_file:
        db = json.load(read_file)
    
    Database.initialise(user=db['user'], password=db['pwd'], 
                        database=db['database'], host=db['host'])

    BASEDIR = '/home/ubuntu/mnt/shared/group'
    BATCH = 1

    logger.info(f'Starting workflow for batch: {BATCH}')
    #update_db(tile_loc=BATCH, level='1C')#, date=('20200220', '20201020'))
    #download(tile_loc=BATCH, basedir=BASEDIR, status='corrupted')
    #correction(tile_loc=BATCH, basedir=BASEDIR)
    stack()
    pca(tile_loc=BATCH, basedir=BASEDIR, proc_status='error pca')


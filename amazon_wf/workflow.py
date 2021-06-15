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

from amazon_wf.actions import download_for_batch
from amazon_wf.actions import stack_for_batch
from amazon_wf.actions import biodivmap_pca_for_batch
from amazon_wf.database import Database

import pdb


logger = logging.getLogger(__name__)

def download():
    for i in range(3):
        logger.info('Starting download procedure..')
        logger.info(f'Run: {i}')
        # connect to the API
        # A .netrc file with username and password must be present in the home folder
        api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
        download_status = download_for_batch(api, BATCH, basedir=BASEDIR)
        if download_status == 1:
            time.sleep(120)
            continue
        elif download_status == 2:
            logger.info('Workflow halted')
            time.sleep(180)
            continue
        elif download_status == 0:
            break

def stack():
    logger.info('Starting pre-processing procedure..')
    stack_for_batch(BATCH, BASEDIR)


def pca():
    logger.info('Starting PCA procedure..')
    biodivmap_pca_for_batch(BATCH, BASEDIR)

if __name__ == "__main__":

    with open('../../db_amazon_credentials.json', "r") as read_file:
        db = json.load(read_file)
    
    Database.initialise(user=db['user'], password=db['pwd'], 
                        database=db['database'], host=db['host'])

    BASEDIR = '/home/ubuntu/mnt/shared/group'
    BATCH = 2

    logger.info(f'Starting workflow for batch: {BATCH}')
    download()
    stack()
    pca()


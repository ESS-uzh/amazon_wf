import os
import pathlib
import time
import logging
import zipfile
import sentinelsat
from sentinelsat import SentinelAPI

from datetime import date
from dateutil.relativedelta import relativedelta

from sentinelsat import SentinelAPI

from amazon_wf.tile import Tile
from amazon_wf.location import Location
from amazon_wf.actions import download_batch

import pdb


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



if __name__ == "__main__":
    BASEDIR = '/home/diego/files/shared/group/ess_biodiversity'
    BATCH = 2

    for i in range(3):
        logger.info('Starting workflow..')
        logger.info(f'Run: {i}')
        # connect to the API
        # A .netrc file with username and password must be present in the home folder
        api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')
        download_status = download_batch(api, BATCH, basedir=BASEDIR)
        if download_status == 1:
            time.sleep(120)
        elif download_status == 2:
            logger.info('Workflow halted')
            time.sleep(180)
        elif download_status == 0:
            logger.info('All done')
            break



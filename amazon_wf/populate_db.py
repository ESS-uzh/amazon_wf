import os
import pathlib
import time
from sentinelsat import SentinelAPI

from datetime import date
from dateutil.relativedelta import relativedelta

from sentinelsat import SentinelAPI

from tile import Tile
from location import Location
from geoRpro.utils import gen_sublist

import pdb


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_tiles_from_file(fpath):
    with open(fpath) as fp:
        content_list = fp.readlines()
    return [line.rstrip("\n") for line in content_list]


def create_batches():
    tiles_db = Tile.get_tiles()
    print(len(tiles_db))
    batch_size = 27
    count = 1
    for batch in gen_sublist(tiles_db, batch_size):
        count_tiles = 0
        for tile_name in batch:
            count_tiles += 1
            tile_db = Tile.load_from_db_by_tile_name(tile_name)
            tile_db.update_tile_loc(count)
        print(f'batch: {count}')
        print(f'n tiles: {count_tiles}')
        count += 1

def create_batch(tiles, batch_number):
    for tile_name in tiles:
        tile_db = Tile(name=tile_name, tile_loc=batch_number)
        tile_db.save_to_db()


#def update_batch_17(api):
#    products = api.query(filename=f'*_{tile}_*',
#             date=('20200720', '20200805'),
#             platformname='Sentinel-2',
#             cloudcoverpercentage=(0, 0.5))
#    if len(products) == 0:
#        f.write(f'No match found\n')
#        continue
#
#    for k in products.keys():
#        begin_acquisition_date = products.get(k)['beginposition'].date()
#        date = begin_acquisition_date.strftime('%Y-%m-%d')
#        size = products.get(k)['size']
#        uuid = products.get(k)['uuid']
#        cloudperc = products.get(k)['cloudcoverpercentage']
#        processlevel = products.get(k)['processinglevel']

if __name__ == "__main__":

    TILES_FP ='./tiles_downloaded.txt' # Change me
    tiles = get_tiles_from_file(TILES_FP)
    # connect to the API
    # A .netrc file with username and password must be present in the home folder
    api = SentinelAPI(None, None, 'https://scihub.copernicus.eu/dhus')

    #create_batch(tiles, 17)
    #populate_db_2alevel(tiles[:10], api)



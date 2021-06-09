import os
from jinja2 import Template
from pathlib import Path
import glob
import logging

import rasterio

from geoRpro.sent2 import Sentinel2
from geoRpro.routines import stack_sent2_bands
import geoRpro.utils as ut

logger = logging.getLogger(__name__)

def generate_hdr(fpath_template, mapping, fpath_out):
    with open(fpath_template, 'r') as f:
        templstr = f.read()
    templ = Template(templstr)
    hdr = templ.render(mapping)
    with open(fpath_out, "w") as fh:
        fh.write(hdr)


def get_satellite_type(fpath_to_sent2_file):
    p=Path(fpath_to_sent2_file)
    _dir = [pa for pa in p.parts if pa.endswith('.SAFE')][0]
    return _dir.split('_')[0]


def pre_process(indir, bands, outdir, hdr2a_fp, hdr2b_fp):

    HDR_data = {}
    s10 = Sentinel2(os.path.join(indir, 'R10m'))
    with rasterio.open(s10.get_fpath('B02_10m')) as src:
        satellite = get_satellite_type(src.files[0])
        crs_raster = src.crs.to_epsg()
    logger.info('Creating a stack..')
    fname = '_'.join([s10.get_tile_number('B02_10m'),
        s10.get_datetake('B02_10m')])+'.tif'
    fpath_stack = stack_sent2_bands(indir, bands, outdir, fname=fname)

    with rasterio.open(fpath_stack) as src_stack:

        width = src_stack.width
        height = src_stack.height
        coords_strg = src_stack.crs.wkt
        u_west = src_stack.transform[2]
        u_north = src_stack.transform[5]

    HDR_data['width'] = width
    HDR_data['height'] = height
    HDR_data['upper_west'] = u_west
    HDR_data['upper_north'] = u_north
    HDR_data['coord_str'] = coords_strg

    fname = '_'.join([s10.get_tile_number('B02_10m'), s10.get_datetake('B02_10m')])+'.hdr'
    if satellite == 'S2A':
        hdr_template_path = hdr2a_fp
    elif satellite == 'S2B':
        hdr_template_path = hdr2b_fp
    generate_hdr(hdr_template_path, HDR_data, os.path.join(outdir, fname))


def get_dirs(indir, tiles_fname):

    # check if indir exist
    try:
        to_process = [glob.glob(os.path.join(d, 'GRANULE/*/IMG_DATA'))[0]
                for d in glob.glob(os.path.join(indir, '*.SAFE'))
                if os.path.basename(d) in tiles_fname]
    except FilenotFoundError as ex:
        raise ex

    return to_process


#   - create the R file from template
def gen_R_script(template_path, mapping, fname_out):
    with open(template_path, 'r') as f:
        templstr = f.read()
        templ = Template(templstr)
        Rscript = templ.render(mapping)
        dir_path = os.path.dirname(template_path)
        fpath_out = os.path.join(dir_path, fname_out)
    with open(fpath_out, "w") as fh:
        fh.write(Rscript)
    return fpath_out


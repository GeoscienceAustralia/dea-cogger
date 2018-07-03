#!/bin/env python
import glob
import json
import logging
import os
import subprocess
import tempfile
from os.path import join as pjoin, basename, dirname, exists, splitext
from subprocess import check_call

import click
import xarray
import yaml
from osgeo import gdal
from pyproj import Proj, transform
from yaml import CLoader as Loader, CDumper as Dumper


# ------------------------------------------------------------------------------
# Code added by Arapaut Sivaprasad
# This function takes the *.yaml files created by the NetCDF to COG conversion.
#	- Get all details required for creating the STAC Json
# 	- Convert the EPSG:3577 coordinates to lat/lon for EPSG:4326 compliance
# 	- Create 'STAC.json' in the same directory as the YAML and TIFF files.
#	- Verify that the newly created STAC.json complies with the latest STAC spec.
# It can now be uploaded to S3 alongwith the rest.
# ------------------------------------------------------------------------------
# FUNCTIONS
# ------------------------------------------------------------------------------
def create_item_dict(item, ard, geodata, base_url):
    """
    Create a dictionary structure of the required values.

    This will be written out as the 'output_dir/subset/item/STAC.json'

    These output files are STAC compliant and must be viewable with any STAC browser.
    """
    item_dict = {
        'id': ard['id'],
        'type': 'Feature',
        'bbox': [ard['extent']['coord']['ll']['lon'], ard['extent']['coord']['ll']['lat'],
                 ard['extent']['coord']['ur']['lon'], ard['extent']['coord']['ur']['lat']],

        'geometry': geodata['geometry'],
        'properties': {
            'datetime': ard['extent']['center_dt'] + '.000Z',
            'provider': 'GA',
            'license': 'PDDL-1.0'
        },
        'links': [
            {
                'rel': 'self',
                'href': base_url + item + "/" + item + ".json"}
        ],
        'assets': {
            'map': {
                'href': base_url + item + '/map.html',
                'required': 'true',
                'type': 'html'
            },
            'metadata': {
                'href': base_url + item + "/ARD-METADATA.yaml",
                "required": 'true',
                "type": "yaml"
            }
        }
    }

    bands = ard['image']['bands']
    for band_num, key in bands:
        path = ard['image']['bands'][key]['path']
        item_dict['assets'][key] = {
            'href': path,
            "required": 'true',
            "type": "GeoTIFF",
            "eo:band": [band_num]}

    return item_dict


def create_geodata(valid_coord):
    """
    The polygon coordinates come in Albers' format, which must be converted to
    lat/lon as in universal format in EPSG:4326
    """
    aa = Proj(init='epsg:3577')
    geo = Proj(init='epsg:4326')
    for i in range(len(valid_coord[0])):
        j = transform(aa, geo, valid_coord[0][i][0], valid_coord[0][i][1])
        valid_coord[0][i] = list(j)

    return {
        'geometry': {
            "type": "Polygon",
            "coordinates": valid_coord
        }
    }


def create_jsons(input_dir, base_url, output_dir, subset):
    """
    Iterate through all tile directories and create a JSON file for each.

    Will look for a *.yaml file in each dir.
    The JSON file will be named as 'base_name.json'
    """
    tiles_list = os.listdir(input_dir)
    for tile_n, item in enumerate(tiles_list):
        tile_dir = pjoin(output_dir, item)
        if os.path.exists(tile_dir):
            os.chdir(tile_dir)
            for yaml_n, ard_metadata_file in enumerate(glob.glob("*.yaml")):
                item_json_file = ard_metadata_file.replace('.yaml', "_STAC.json")
                try:
                    with open(ard_metadata_file) as ard_src:
                        ard_metadata = yaml.safe_load(ard_src)

                    geodata = create_geodata(ard_metadata['grid_spatial']['projection']['valid_data']['coordinates'])
                    item_dict = create_item_dict(item, ard_metadata, geodata, base_url)

                    # Write out the JSON files.
                    with open(item_json_file, 'w') as dest:
                        json.dump(dest, item_dict, indent=1)
                        print("Wrote: {}_{}. {}".format(tile_n, yaml_n, item_json_file))
                    # Write out only if the file does not exist.
                #                    if (os.path.exists(item_json_file) and os.path.getsize(item_json_file) > 0):
                #                        print("*** File exits. Not overwriting:", item_json_file)
                #                    else:
                #                        with open(item_json_file, 'w') as file:
                #                             file.write(json.dumps(item_dict,indent=1))
                #                    break
                except:
                    print("*** Unknown error in loading/writing the data.", ard_metadata_file)


def stac_create(input_dir, subset, output_dir):
    print("Creating the STAC.json files!")
    base_url = "http://dea-public-data.s3-ap-southeast-2.amazonaws.com/FCP"

    # DRA: I don't understand the intended effect of the following three lines
    base_url = os.path.join(base_url, '')
    output_dir = os.path.join(output_dir, '')
    input_dir = os.path.join(input_dir, '')
    create_jsons(input_dir, base_url, output_dir, subset)


# ------------------------------------------------------------------------------
# Code below is the original and has not been altered in any way from the original,
# but for a line added in 'def main()' to call 'stac_create()'
# ------------------------------------------------------------------------------
def run_command(command, work_dir):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=work_dir)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def check_dir(fname):
    file_name = fname.split('/')
    rel_path = pjoin(*file_name[-2:])
    file_wo_extension, extension = splitext(rel_path)
    return file_wo_extension


def getfilename(fname, outdir):
    """ To create a temporary filename to add overviews and convert to COG
        and create a file name just as source but without '.TIF' extension
    """
    file_path = check_dir(fname)
    out_fname = pjoin(outdir, file_path)
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))
    return out_fname, file_path


def get_bandname(filename):
    return (filename.split(':'))[-1]


def add_image_path(bands, fname, rc, count):
    for key, value in bands.items():
        value['layer'] = '1'
        if rc > 1:
            value['path'] = basename(fname) + '_' + str(count + 1) + '_' + key + '.tif'
        else:
            value['path'] = basename(fname) + '_' + key + '.tif'
    return bands


def _write_dataset(fname, file_path, outdir, rastercount):
    """ Write the dataset which is in indexable format to datacube and update the format name too GeoTIFF"""
    dataset_array = xarray.open_dataset(fname)
    for count in range(rastercount):
        if rastercount > 1:
            y_fname = file_path + '_' + str(count + 1) + '.yaml'
            dataset_object = (dataset_array.dataset.item(count)).decode('utf-8')
        else:
            y_fname = file_path + '.yaml'
            dataset_object = (dataset_array.dataset.item()).decode('utf-8')
        yaml_fname = pjoin(outdir, y_fname)
        dataset = yaml.load(dataset_object, Loader=Loader)
        bands = dataset['image']['bands']
        dataset['image']['bands'] = add_image_path(bands, file_path, rastercount, count)
        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}
        with open(yaml_fname, 'w') as fp:
            yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
            logging.info("Writing dataset Yaml to %s", basename(yaml_fname))


def _write_cogtiff(out_f_name, outdir, subdatasets, rastercount):
    """ Convert the Geotiff to COG using gdal commands
        Blocksize is 512
        TILED <boolean>: Switch to tiled format
        COPY_SRC_OVERVIEWS <boolean>: Force copy of overviews of source dataset
        COMPRESS=[NONE/DEFLATE]: Set the compression to use. DEFLATE is only available if NetCDF has been compiled with
                  NetCDF-4 support. NC4C format is the default if DEFLATE compression is used.
        ZLEVEL=[1-9]: Set the level of compression when using DEFLATE compression. A value of 9 is best,
                      and 1 is least compression. The default is 1, which offers the best time/compression ratio.
        BLOCKXSIZE <int>: Tile Width
        BLOCKYSIZE <int>: Tile/Strip Height
        PREDICTOR <int>: Predictor Type (1=default, 2=horizontal differencing, 3=floating point prediction)
        PROFILE <string-select>: possible values: GDALGeoTIFF,GeoTIFF,BASELINE,
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        for netcdf in subdatasets[:-1]:
            for count in range(1, rastercount + 1):
                band_name = get_bandname(netcdf[0])
                if rastercount > 1:
                    out_fname = out_f_name + '_' + str(count) + '_' + band_name + '.tif'
                else:
                    out_fname = out_f_name + '_' + band_name + '.tif'

                env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
                       'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
                subprocess.check_call(env, shell=True)

                # copy to a tempfolder
                temp_fname = pjoin(tmpdir, basename(out_fname))
                to_cogtif = [
                    'gdal_translate',
                    '-b',
                    str(count),
                    netcdf[0],
                    temp_fname]
                run_command(to_cogtif, tmpdir)

                # Add Overviews
                # gdaladdo - Builds or rebuilds overview images.
                # 2, 4, 8,16,32 are levels which is a list of integral overview levels to build.
                add_ovr = [
                    'gdaladdo',
                    '-r',
                    'average',
                    '--config',
                    'GDAL_TIFF_OVR_BLOCKSIZE',
                    '512',
                    temp_fname,
                    '2',
                    '4',
                    '8',
                    '16',
                    '32']
                run_command(add_ovr, tmpdir)

                # Convert to COG
                cogtif = [
                    'gdal_translate',
                    '-co',
                    'TILED=YES',
                    '-co',
                    'COPY_SRC_OVERVIEWS=YES',
                    '-co',
                    'COMPRESS=DEFLATE',
                    '-co',
                    'ZLEVEL=9',
                    '--config',
                    'GDAL_TIFF_OVR_BLOCKSIZE',
                    '512',
                    '-co',
                    'BLOCKXSIZE=512',
                    '-co',
                    'BLOCKYSIZE=512',
                    '-co',
                    'PREDICTOR=1',
                    '-co',
                    'PROFILE=GeoTIFF',
                    temp_fname,
                    out_fname]
                run_command(cogtif, outdir)


@click.command(help="\b Convert netcdf to Geotiff and then to Cloud Optimized Geotiff using gdal."
                    " Mandatory Requirement: GDAL version should be >=2.2")
@click.option('--path', '-p', required=True, help="Read the netcdfs from this folder",
              type=click.Path(exists=True, readable=True))
@click.option('--output', '-o', required=True, help="Write COG's into this folder",
              type=click.Path(exists=True, writable=True))
def main(path, output):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    netcdf_path = os.path.abspath(path)
    output_dir = os.path.abspath(output)
    skip_cog = 1  # Debug by AVS to skip the NetCDF to COG creation step. Change it to 0 OR remove in prod.
    if not skip_cog:
        for path, subdirs, files in os.walk(netcdf_path):
            for fname in files:
                f_name = pjoin(path, fname)
                logging.info("Reading %s", basename(f_name))
                gtiff_fname, file_path = getfilename(f_name, output_dir)
                dataset = gdal.Open(f_name, gdal.GA_ReadOnly)
                subdatasets = dataset.GetSubDatasets()
                # ---To Check if NETCDF is stacked or unstacked --
                sds_open = gdal.Open(subdatasets[0][0])
                rastercount = sds_open.RasterCount
                dataset = None
                _write_dataset(f_name, file_path, output_dir, rastercount)
                _write_cogtiff(gtiff_fname, output_dir, subdatasets, rastercount)
                logging.info("Writing COG to %s", basename(gtiff_fname))

    # Code added by Arapaut Sivaprasad
    stac_create(netcdf_path, '', output_dir)


if __name__ == "__main__":
    main()

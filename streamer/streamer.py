#!/usr/bin/env python
"""
Batch Convert NetCDF files to Cloud-Optimised-GeoTIFF and upload to S3

This tool is broken into 3 pieces:

 1) Work out the difference between NetCDF files stored locally, and GeoTIFF files in S3
 2) Batch convert NetCDF files into Cloud Optimised GeoTIFFs
 3) Watch a directory and upload files to S3


Finding files to process
------------------------
This can either be done manually with a command like `find <dir> --name '*.nc'`, or
by searching an ODC Index using::

    python streamer.py generate_work_list --product-name <name> [--year <year>] [--month <month>]

This will print a list of NetCDF files which can be piped to `convert_cog`.


Batch Converting NetCDF files
-----------------------------
::

    python streamer.py convert_cog [--max-procs <int>] --config <file> --product <product> --output-dir <dir> List of NetCDF files...

Use the settings in a configuration file to:

- Parse variables from the NetCDF filename/directory
- Generate output directory structure and filenames
- Configure COG Overview resampling method

When run, each `ODC Dataset` in each NetCDF file will be converted into an output directory containing a COG
for each `band`, as well as a `.yaml` dataset definition, and a `upload-destination.txt` file containing
the full destination directory.

During processing, `<output-directory/WORKING/` will contain in-progress Datasets.
Once a Dataset is complete, it will be moved into the `<output-directory>/TO_UPLOAD/`



Uploading to S3
---------------

Watch `<output-directory>/TO_UPLOAD/` for new COG Dataset Directories, and upload them to the `<upload-destination>`.

Once uploaded, directories can either be deleted or moved elsewhere for safe keeping.




Configuration
-------------

The program uses a config, that in particular specify product descriptions such as whether time values are taken
from filename or dateset or there no time associated with datasets, source and destination filename templates,
aws directory, dataset specific aws directory suffix, resampling method for cog conversion.
The destination template must only specify the prefix of the file excluding the band name details and
extension. An example such config spec for a product is as follows:

    ls5_fc_albers:
        time_taken_from: dataset
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        resampling_method: average

"""
import logging
import os
import re
import subprocess
import tempfile
import time
import sys
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from os.path import join as pjoin, basename, dirname, exists
from pathlib import Path
from subprocess import check_output, run, check_call

import click
import gdal
import xarray
import yaml
from datacube import Datacube
from datacube.model import Range
from netCDF4 import Dataset
from pandas import to_datetime
from tqdm import tqdm
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from parse import *
from parse import compile
from cogeo import *

import re


WORKERS_POOL = 4

DEFAULT_CONFIG = """
products: 
    wofs_albers: 
        time_taken_from: filename
        src_template: LS_WATER_3577_{x}_{y}_{time}_v{}.nc 
        dest_template: LS_WATER_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf
        aws_dir: WOfS/WOFLs/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: mode
    wofs_filtered_summary:
        time_taken_from: notime
        src_template: wofs_filtered_summary_{x}_{y}.nc
        dest_template: wofs_filtered_summary_{x}_{y}
        src_dir: /g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf
        aws_dir: WOfS/filtered_summary/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}
        bands_to_cog_convert: [confidence]
        default_resampling_method: mode
        band_resampling_methods: {confidence: mode}
    wofs_annual_summary:
        time_taken_from: filename
        src_template: WOFS_3577_{x}_{y}_{time}_summary.nc
        dest_template: WOFS_3577_{x}_{y}_{time}_summary
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_Stats_Ann_25_2_1/netcdf
        aws_dir: WOfS/annual_summary/v2.1.5/combined
        aws_dir_suffix: x_{x}/y_{y}/{year}
        bucket: s3://dea-public-data-dev
        default_resampling_method: mode
    ls5_fc_albers:
        time_taken_from: dataset
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
    ls7_fc_albers:
        time_taken_from: dataset
        src_template: LS7_ETM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS7_ETM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS7_ETM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls7
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
    ls8_fc_albers:
        time_taken_from: dataset
        src_template: LS8_OLI_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS8_OLI_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS8_OLI_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls8
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
    fcp_cog:
        dest_template: x_{x}/y_{y}/{year}
        nonpym_list: ["source", "observed"]
"""


def run_command(command, work_dir=None):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=None, env=os.environ)
        #run(command, cwd=work_dir, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
    """
    def __init__(self, black_list=None, white_list=None, nonpym_list=None, default_rsp=None, 
            bands_rsp=None, dest_template=None, src_template=None, predictor=None):
        self.nonpym_list = nonpym_list 
        self.black_list = black_list
        self.white_list = white_list
        if predictor is None:
            self.predictor = 2
        else:
            self.predictor = predictor
        if default_rsp is None:
            self.default_rsp = 'average'
        else:
            self.default_rsp = default_rsp
        self.bands_rsp = bands_rsp 
        if dest_template is None:
            self.dest_template = "x_{x}/y_{y}/{year}"
        else:
            self.dest_template = dest_template
        if src_template is None:
            self.src_template = "{x}_{y}_{time}"
        else:
            self.src_template = src_template

    """
    call to convert 
    """
    def __call__(self, input_fname, dest_dir):

        prefix_name = self.make_out_prefix(input_fname, dest_dir)
        self.netcdf_to_cog(input_fname, prefix_name)

    def make_out_prefix(self, input_fname, dest_dir):
        abs_fname = basename(input_fname) 
        r = re.compile(r"(?<=_)[-\d]+")
        indices = r.findall(abs_fname)
        r = re.compile(r"\{\w+\}")
        key_indices = r.findall(self.src_template)
        if len(key_indices) > 3:
            x_index, y_index, datetime = indices[-len(key_indices):-(len(key_indices) - 3)]
        else:
            x_index, y_index, datetime = indices[-len(key_indices):]

        time_dict = {}
        year = re.search(r"\d{4}", datetime)
        month = re.search(r'(?<=\d{4})\d{2}', datetime)
        day = re.search(r'(?<=\d{6})\d{2}', datetime)
        time = re.search(r'(?<=\d{8})\d+', datetime)
        if year is not None:
            time_dict['year'] = year.group(0) 
        if month is not None:
            time_dict['month'] = month.group(0)
        if day is not None:
            time_dict['day'] = day.group(0)        
        if time is not None:
            time_dict['time'] = time.group(0)

        out_dir = pjoin(dest_dir,  self.dest_template.format(x=x_index, y=y_index, **time_dict))
        os.makedirs(out_dir, exist_ok=True)

        prefix_name = re.search(r"[\wd-]*(?<=.)", abs_fname).group(0)
        return pjoin(out_dir, prefix_name)


    def netcdf_to_cog(self, input_file, prefix):
        """
        Convert the datasets in the NetCDF file 'file' into 'dest_dir'

        Each dataset is put in a separate directory.

        The directory names will look like 'LS_WATER_3577_9_-39_20180506102018'
        """
        
        try:
            dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        except:
            LOG.info("netcdf error: %s", input_file)
            return

        if dataset is None:
            return

        subdatasets = dataset.GetSubDatasets()

        # Extract each band from the NetCDF and write to individual GeoTIFF files
        rastercount = self._dataset_to_cog(prefix, subdatasets)

        dataset_array = xarray.open_dataset(input_file)
        self._dataset_to_yaml(prefix, dataset_array, rastercount)
        # Clean up XML files from GDAL
        # GDAL creates extra XML files which we don't want


    def _dataset_to_yaml(self, prefix, dataset_array: xarray.DataArray, rastercount):
        """
        Write the datasets to separate yaml files
        """
        for i in range(rastercount):
            if rastercount == 1:
                yaml_fname = prefix + '.yaml'
                dataset_object = (dataset_array.dataset.item()).decode('utf-8')
            else:
                yaml_fname = prefix + '_' + str(i+1) + '.yaml'
                dataset_object = (dataset_array.dataset.item(i)).decode('utf-8')

            if exists(yaml_fname):
                continue

            dataset = yaml.load(dataset_object, Loader=Loader)
            if dataset is None:
                LOG.info("No yaml section %s", prefix)
                continue

            # Update band urls
            for key, value in dataset['image']['bands'].items():
                if rastercount == 1:
                    tif_path = basename(prefix + '_' + key + '.tif')
                else:
                    tif_path = basename(prefix  + '_' + key + '_' + str(i+1) + '.tif')

                value['layer'] = str(i+1)
                value['path'] = tif_path

            dataset['format'] = {'name': 'GeoTIFF'}
            dataset['lineage'] = {'source_datasets': {}}
            with open(yaml_fname, 'w') as fp:
                yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)


    def _dataset_to_cog(self, prefix, subdatasets):
        """
        Write the datasets to separate cog files
        """

        os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
        os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'
        if self.white_list is not None:
            re_white = "|".join(self.white_list)
        if self.black_list is not None:
            re_black = "|".join(self.black_list)
        if self.nonpym_list is not None:
            re_nonpym = "|".join(self.nonpym_list)

        rastercount = 0
        for dts in subdatasets[:-1]:
            rastercount = gdal.Open(dts[0]).RasterCount
            for i in range(rastercount):
                band_name = dts[0].split(':')[-1]

                # Only do specified bands if specified
                if self.black_list is not None:
                    if re.search(re_black, band_name) is not None:
                        continue

                if self.white_list is not None:
                    if re.search(re_white, band_name) is None:
                        continue

                if rastercount == 1:
                    out_fname = prefix + '_' + band_name + '.tif'
                else:
                    out_fname = prefix + '_' + band_name + '_' + str(i+1) + '.tif'

                # Check the done files might need a force option later
                if exists(out_fname):
                    if self._check_tif(out_fname):
                        continue

                # Resampling method of this band
                resampling_method = None
                if self.bands_rsp is not None:
                    resampling_method = self.bands_rsp.get(band_name)
                if resampling_method is None:
                    resampling_method = self.default_rsp
                if self.nonpym_list is not None:
                   if re.search(re_nonpym, band_name) is not None:
                        resampling_method = None

                default_profile = {'driver': 'GTiff',
                            'interleave': 'pixel',
                            'tiled': True,
                            'blockxsize': 512,
                            'blockysize': 512,
                            'compress': 'DEFLATE',
                            'predictor': self.predictor,
                            'zlevel': 9}
                default_config = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}

                cog_translate(dts[0], out_fname,
                        default_profile,
                        indexes=[i+1],
                        overview_resampling=resampling_method,
                        overview_level=5,
                        config=default_config)

        return rastercount


    def _check_tif(self, fname):
        try:
            cog_tif = gdal.Open(fname, gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except:
            cog_tif = None
            return False

        cog_tif = None
        if t_stats > [0.]*4:
            return True
        else:
            return False



class COGProductConfiguration:
    """
    Utilities and some hardcoded stuff for tracking and coding job info.

    :param dict cfg: Configuration for the product we're processing
    """

    def __init__(self, cfg):
        self.cfg = cfg


def get_indexed_files(product, year=None, month=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    query = {'product': product}
    if year and month:
        query['time'] = Range(datetime(year=year, month=month, day=1), datetime(year=year, month=month + 1, day=1))
    elif year:
        query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    dc = Datacube(app='streamer', env=datacube_env)
    files = dc.index.datasets.search_returning(field_names=('uri',), **query)

    # TODO: For now, turn the URL into a file name by removing the schema and #part. Should be made more robust
    def filename_from_uri(uri):
        return uri[0].split(':')[1].split('#')[0]

    return set(filename_from_uri(uri) for uri in files)


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
def generate_work_list(product_name, year, month):
    """
    Connect to an ODC database and list NetCDF files
    """
    items_all = get_indexed_files(product_name, year, month)

    for item in sorted(items_all):
        print(item)

try:
    from mpi4py import MPI
except:
    LOG.warning("mpi4py is not available")

@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--flist', '-l', help='List of file names', default=None)
@click.argument('filenames', nargs=-1, type=click.Path())
def convert_cog(config, output_dir, product, flist, filenames):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format

    Uses a configuration file to define the file naming schema.

    """
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    product_config = cfg['products'][product]

    cog_convert = COGNetCDF(**product_config)

    if flist is not None:
        with open(flist, 'r') as fb:
            file_list =  np.genfromtxt(fb, dtype='str')
    else:
        file_list = list(filenames)

    try:
        comm = MPI.Comm.Get_parent()
        size = comm.Get_size()
        rank = comm.Get_rank()
    except:
        LOG.info("Run with single process")
        for filename in file_list:
            cog_convert(filename, output_dir)
    else:
        comm.Merge(True)
        batch_size = int(len(file_list)/size)
        for filename in file_list[rank*batch_size:(rank+1)*batch_size]:
            cog_convert(filename, output_dir)
        comm.Disconnect()


from time import sleep
@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--numprocs', type=int, help='Number of processes', required=True, default=1)
@click.option('--cog-path', help='cog convert script path', required=True, default='../COG-Conversion/streamer/streamer.py')
@click.argument('filelist', nargs=1, required=True)
def mpi_convert_cog(config, output_dir, product, numprocs, cog_path, filelist):
    comdLine = [cog_path] + ['convert_cog', '-c'] + [config] + ['--output-dir'] + [output_dir] + ['--product' ] + [product]
    args = comdLine
    with open(filelist, 'r') as fb:
        file_list =  np.genfromtxt(fb, dtype='str')
    LOG.debug("Process file %s", filelist)
    file_odd = len(file_list) % numprocs
    LOG.debug("file_odd %d", file_odd)
    margs = args + ['-l', filelist] 
    while True:
        try:
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                args=margs,
                                maxprocs=numprocs)

        except:
            sleep(1)
        else:
            comm.Merge()
            break
    comm.Disconnect()
    LOG.debug("Batch done")
    if file_odd > 0:
        numprocs = file_odd
        margs = args + list(file_list[-file_odd:])
        while True:
            try:
                comm = MPI.COMM_SELF.Spawn(sys.executable,
                                        args=margs,
                                        maxprocs=numprocs)
            except:
                sleep(1)
            else:
                comm.Merge()
                break
        comm.Disconnect()
    LOG.debug("Job done") 

if __name__ == '__main__':
    LOG = logging.getLogger(__name__)
    LOG.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    LOG.addHandler(ch)

    cli()

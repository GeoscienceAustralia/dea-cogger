
import logging
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from os.path import join as pjoin, basename
from pathlib import Path
from subprocess import check_output, run

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

from .streamer import COGProductConfiguration, get_indexed_files

import boto3

LOG = logging.getLogger(__name__)

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
        resampling_method: mode
    wofs_filtered_summary:
        time_taken_from: notime
        src_template: wofs_filtered_summary_{x}_{y}.nc
        dest_template: wofs_filtered_summary_{x}_{y}
        src_dir: /g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf
        aws_dir: WOfS/filtered_summary/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}
        resampling_method: mode
    wofs_annual_summary:
        time_taken_from: filename
        src_template: WOFS_3577_{x}_{y}_{time}_summary.nc
        dest_template: WOFS_3577_{x}_{y}_{time}_summary
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_Stats_Ann_25_2_1/netcdf
        aws_dir: WOfS/annual_summary/v2.1.5/combined
        aws_dir_suffix: x_{x}/y_{y}/{year}
        resampling_method: mode
        bucket: s3://dea-public-data-dev
    ls5_fc_albers:
        time_taken_from: dataset
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        resampling_method: average
    ls7_fc_albers:
        time_taken_from: dataset
        src_template: LS7_ETM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS7_ETM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS7_ETM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls7
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        resampling_method: average
    ls8_fc_albers:
        time_taken_from: dataset
        src_template: LS8_OLI_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS8_OLI_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS8_OLI_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls8
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        resampling_method: average
"""


def run_command(command, work_dir=None):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        run(command, cwd=work_dir, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def get_indexed_info(product, year=None, month=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    query = {'product': product}
    if year and month:
        query['time'] = Range(datetime(year=year, month=month, day=1), datetime(year=year, month=month + 1, day=1))
    elif year:
        query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    dc = Datacube(app='streamer', env=datacube_env)
    dts = dc.index.datasets.search_returning(field_names=('id', 'uri'), **query)

    # TODO: For now, turn the URL into a file name by removing the schema and #part. Should be made more robust
    def filename_from_uri(uri):
        return uri[0].split(':')[1].split('#')[0]

    # TODO: uniqueness of (id, uri) combination
    return ((dt[0], filename_from_uri(dt[1])) for dt in dts)


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--bucket', '-b', required=True, type=click.Path(), help="AWS bucket")
def check_nci_to_s3(config, product_name, year, month, bucket):

    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)
    product_config = COGProductConfiguration(cfg['products'][product_name])

    items_all = get_indexed_files(product_name, year, month)

    conn = boto3.client('s3')
    keys = []
    keys_short =[]
    kwargs = {'Bucket': bucket}

    for item in items_all:
        prefix_names = product_config.get_unstacked_names(item, year, month)
        for prefix in prefix_names:
            aws_dir = product_config.cfg['aws_dir']
            s3_object_prefix = f'{aws_dir}/{product_config.aws_dir_suffix(prefix)}/{prefix}'
            while True:
                resp = conn.list_objects_v2(**kwargs, Prefix=s3_object_prefix)
                for obj in resp['Contents']:
                    key = obj['Key']
                    if check_key(key, prefix):
                        # ToDo
                        pass
                try:
                    kwargs['ContinuationToken'] = resp['NextContinuationToken']
                except KeyError:
                    break


if __name__ == '__main__':
    cli()

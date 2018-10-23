import logging
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, wait, as_completed
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
from datacube.utils import netcdf_extract_string
from netCDF4 import Dataset
from pandas import to_datetime
from tqdm import tqdm
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from parse import *
from parse import compile

from streamer import COGProductConfiguration, DEFAULT_CONFIG

import boto3

LOG = logging.getLogger(__name__)

WORKERS_POOL = 4


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
        return uri.split(':')[1].split('#')[0]

    # TODO: uniqueness of (id, uri) combination
    return ((dt[0], filename_from_uri(dt[1])) for dt in dts)


def subset_of_s3_keys(key_set, prefix, product):
    dc = Datacube(app='aws_check')
    product_ = dc.index.products.get_by_name(product)
    file_names = set()
    if product_:
        # tif files
        file_names = {prefix + '_' + band[0] + '.tif' for band in product_.measurements.items()}
        # the yaml file
        file_names.update([prefix + '.yaml'])
    return file_names.issubset(key_set)


def get_prefixes(uuid, netcdf_file, config):
    # This function is too slow needs speed up
    netcdf_dataset = Dataset(netcdf_file)
    dts_dataset = netcdf_dataset.variables['dataset']
    dts_time = netcdf_dataset.variables['time']
    for index, dt_ in enumerate(dts_dataset):
        dt = yaml.load(netcdf_extract_string(dt_.data), Loader)
        if str(uuid) == dt['id']:
            # Construct prefix(es)
            dt_time = datetime.fromtimestamp(dts_time[index])
            time_stamp = to_datetime(dt_time).strftime('%Y%m%d%H%M%S%f')
            year_ = time_stamp[0:4]
            month_ = time_stamp[4:6]
            day = time_stamp[6:8]
            src_file_param_values = parse(config['src_template'], basename(netcdf_file)).__dict__['named']
            time_param_values = {'time': time_stamp, 'year': year_, 'month': month_, 'day': day}

            # All available parameter values
            all_param_values = dict(src_file_param_values, **time_param_values)

            # ToDo: dest_template of prior uploads
            return [config['dest_template'].format(**all_param_values)]


def get_expected_list(product_config, product_name, year, month):
    items_all = list(get_indexed_info(product_name, year, month))[0:200]
    with Datacube(app='aws_check') as dc:
        bands = dc.index.products.get_by_name(product_name).measurements.items()
    file_names = set()
    with ProcessPoolExecutor(max_workers=WORKERS_POOL) as executor:
        futures = (
            executor.submit(get_prefixes, uuid, filename, product_config.cfg)
            for uuid, filename in items_all
        )

        for future in as_completed(futures):
            prefix = future.result()
            aws_dir = product_config.cfg['aws_dir']
            aws_object_prefix = f'{aws_dir}/{product_config.aws_dir_suffix(prefix[0])}'
            file_names.update(
                {f'{aws_object_prefix}/{prefix[0] + "_" + band[0] + ".tif"}' for band in bands}
            )

    return file_names


def has_x_y(config):
    aws_dir_suffix_template = config.cfg['aws_dir_suffix']
    aws_dir_params = compile(aws_dir_suffix_template)._named_fields
    return 'x' in aws_dir_params and 'y' in aws_dir_params


def aws_search_template(config, year=None, month=None):
    date_str = datetime(year=year, month=month, day=1).strftime('%Y%m')
    year_ = date_str[0:4]
    month_ = date_str[4:6]
    aws_dir_suffix_template = config.cfg['aws_dir_suffix']
    aws_dir_params = compile(aws_dir_suffix_template)._named_fields
    if 'x' in aws_dir_params and 'y' in aws_dir_params:
        if month and 'month' in aws_dir_params:
            prefix_template = 'x_{x}/y_{y}/' + year_ + '/' + month_ + '/'
        elif year and 'year' in aws_dir_params:
            prefix_template = 'x_{x}/y_{y}/' + year_ + '/'
        else:
            prefix_template = ''
    else:
        prefix_template = ''
    top_level_aws_dir = config.cfg['aws_dir']
    if not top_level_aws_dir[-1] == '/':
        top_level_aws_dir += '/'
    return f'{top_level_aws_dir}{prefix_template}'


def get_aws_list(config, bucket, year=None, month=None):
    conn = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    top_level_aws_dir = config.cfg['aws_dir']
    if not top_level_aws_dir[-1] == '/':
        top_level_aws_dir += '/'
    aws_object_list = []
    search_prefix_template = aws_search_template(config, year, month)

    # ToDo: continuation of list_objects_v2
    if year:
        x_y_list = []
        if has_x_y(config):
            # get x list
            resp = conn.list_objects_v2(**kwargs, Prefix=top_level_aws_dir + 'x_', Delimiter='/')
            x_list = (parse(top_level_aws_dir + 'x_{x}/', item['Prefix']).__dict__['named']['x']
                      for item in resp.get('CommonPrefixes'))
            # get y list for given x
            for x in x_list:
                resp = conn.list_objects_v2(**kwargs, Prefix=top_level_aws_dir + 'x_{}/y_'.format(x), Delimiter='/')
                y_list = (parse(top_level_aws_dir + 'x_' + x + '/y_{y}/', item['Prefix']).__dict__['named']['y']
                          for item in resp.get('CommonPrefixes'))
                x_y_list.extend(((x, y) for y in y_list))

        for x, y in x_y_list:
            resp = conn.list_objects_v2(**kwargs, Prefix=search_prefix_template.format(x=x, y=y))
            if not resp['KeyCount'] == 0:
                aws_object_list.extend((item['Key'] for item in resp['Contents']))
        return aws_object_list
    else:
        resp = conn.list_objects_v2(**kwargs, Prefix=search_prefix_template)
        return [item['Key'] for item in resp['Contents']]


def compare_nci_with_aws(config, product_name, year, month, bucket, output_file):
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)
    config = COGProductConfiguration(cfg['products'][product_name])
    aws_set = set(get_aws_list(config, bucket, year, month))
    expected_set = set(get_expected_list(config, product_name, year, month))
    return {'aws_but_not_nci': aws_set - expected_set, 'nci_but_not_aws': expected_set - aws_set}


def _check_item(uuid, filename, product_config, output_file, product_name, bucket):
    conn = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    prefix = get_prefixes(uuid, filename, product_config)[0]
    aws_dir = product_config.cfg['aws_dir']
    s3_object_prefix = f'{aws_dir}/{product_config.aws_dir_suffix(prefix)}/{prefix}'

    # It is assumed that response does not have continuation response
    resp = conn.list_objects_v2(**kwargs, Prefix=s3_object_prefix)
    if resp['KeyCount'] == 0:
        with open(output_file, 'a') as output:
            output.write(yaml.dump({'uuid': uuid, 'prefix': prefix, 'file': filename}))
    else:
        key_set = {basename(obj['Key']) for obj in resp['Contents']}
        if not subset_of_s3_keys(key_set, prefix, product_name):
            with open(output_file, 'a') as output:
                output.write(yaml.dump({'uuid': uuid, 'prefix': prefix, 'file': filename}))


def check_nci_to_s3_(config, product_name, year, month, bucket, output_file):

    # if config:
    #     with open(config, 'r') as cfg_file:
    #         cfg = yaml.load(cfg_file)
    # else:
    #     cfg = yaml.load(DEFAULT_CONFIG)
    # product_config = COGProductConfiguration(cfg['products'][product_name])
    #
    # items_all = get_indexed_info(product_name, year, month)
    #
    # with ProcessPoolExecutor(max_workers=WORKERS_POOL) as executor:
    #
    #     futures = [
    #         executor.submit(_check_item, uuid, filename, product_config, output_file, product_name, bucket)
    #         for uuid, filename in items_all
    #     ]
    #     wait(futures)
    pass


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--bucket', '-b', required=True, type=click.Path(), help="AWS bucket")
@click.argument('--output_file', type=click.Path())
def check_nci_to_s3(config, product_name, year, month, bucket, output_file):
    check_nci_to_s3_(config, product_name, year, month, bucket, output_file)


if __name__ == '__main__':
    cli()

import logging
import subprocess
import tempfile
from concurrent.futures import ProcessPoolExecutor, wait, as_completed
from datetime import datetime, timedelta
from os.path import join as pjoin, basename
from subprocess import run

import click
import yaml
from datacube import Datacube
from datacube.model import Range
from datacube.utils import netcdf_extract_string
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
    dts = dc.index.datasets.search_returning(field_names=('id', 'uri', 'time'), **query)

    # TODO: For now, turn the URL into a file name by removing the schema and #part. Should be made more robust
    def filename_from_uri(uri):
        return uri.split(':')[1].split('#')[0]

    return ((dt[0], filename_from_uri(dt[1]), dt[2].lower) for dt in dts)


def get_prefixes(netcdf_file, dataset_time, config):

    time_stamp = dataset_time.strftime('%Y%m%d%H%M%S')
    year_ = time_stamp[0:4]
    month_ = time_stamp[4:6]
    day = time_stamp[6:8]
    src_file_param_values = parse(config['src_template'], basename(netcdf_file)).__dict__['named']
    time_param_values = {'time': time_stamp, 'year': year_, 'month': month_, 'day': day}

    # All available parameter values
    all_param_values = dict(src_file_param_values, **time_param_values)

    return [config['dest_template'].format(**all_param_values)]


def get_expected_list(product_config, product_name, year, month):

    items_all = list(get_indexed_info(product_name, year, month))
    with Datacube(app='aws_check') as dc:
        bands = dc.index.products.get_by_name(product_name).measurements.items()
    file_names = set()
    for uuid, filename, dataset_time in items_all:
        prefixes = get_prefixes(filename, dataset_time, product_config.cfg)
        aws_dir = product_config.cfg['aws_dir']
        aws_object_prefix = f'{aws_dir}/{product_config.aws_dir_suffix(prefixes[0])}'
        file_names.update(
            {f'{aws_object_prefix}/{prefixes[0] + "_" + band[0] + ".tif"}' for band in bands}
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


def get_all_s3_with_prefix(connection, kwargs, prefix):
    kwargs_ = kwargs.copy()
    aws_object_list = []
    while True:
        resp = connection.list_objects_v2(**kwargs_, Prefix=prefix)
        if resp.get('KeyCount'):
            aws_object_list.extend([item['Key'] for item in resp.get('Contents')])
        try:
            kwargs_['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return aws_object_list


def get_aws_list(config, bucket, year=None, month=None):
    conn = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    top_level_aws_dir = config.cfg['aws_dir']
    if not top_level_aws_dir[-1] == '/':
        top_level_aws_dir += '/'
    aws_object_list = []
    search_prefix_template = aws_search_template(config, year, month)

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
            aws_object_list.extend(get_all_s3_with_prefix(conn, kwargs, search_prefix_template.format(x=x, y=y)))
        return aws_object_list
    else:
        return get_all_s3_with_prefix(conn, kwargs, search_prefix_template)


def compare_nci_with_aws(cfg, product_name, year, month, bucket, output_file):
    config = COGProductConfiguration(cfg['products'][product_name])
    aws_set = set(get_aws_list(config, bucket, year, month))
    expected_set = set(get_expected_list(config, product_name, year, month))
    return {'aws_but_not_nci': aws_set - expected_set, 'nci_but_not_aws': expected_set - aws_set}


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
def check_nci_with_s3(config, product_name, year, month, bucket, output_file):
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)
    print(compare_nci_with_aws(cfg, product_name, year, month, bucket, output_file))


if __name__ == '__main__':
    cli()

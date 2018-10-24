"""
This script compare expected AWS s3 objects with what is actually present in s3
The lines that are printed to standard output has the following structure:

'missing-in-aws:<uuid>:<expected_s3_object>
OR
'excess-aws-file:<s3_object>'

E.g.
'missing-in-aws:2f99b666-8079-4a62-84c7-ebf3db402933:WOfS/WOFLs/v2.1.0/combined/x_-8/y_-36/2018/07/12/'
                                                               'LS_WATER_3577_-8_-36_20180712114018_water.tif'

OR

'excess-aws-file:WOfS/WOFLs/v2.1.0/combined/x_-8/y_-36/2018/07/12/LS_WATER_3577_-8_-36_20180712114018_water.tif'

"""

from os.path import basename
import click
import yaml
from datacube import Datacube
from datacube.model import Range

from parse import *
from parse import compile
import boto3

from streamer import COGProductConfiguration, DEFAULT_CONFIG
from datetime import datetime

import logging
LOG = logging.getLogger(__name__)


def get_indexed_info(product, year=None, month=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    Returns a list of tuples (id, uri, time)
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
    """
    Construct the prefix (i.e. excluding band name and extension) like 'LS_WATER_3577_9_-39_20180506102018'
    Extract the grid index (cell index) from the given file
    """

    time_stamp = dataset_time.strftime('%Y%m%d%H%M%S')
    year_ = time_stamp[0:4]
    month_ = time_stamp[4:6]
    day = time_stamp[6:8]
    src_file_param_values = parse(config['src_template'], basename(netcdf_file)).__dict__['named']
    time_param_values = {'time': time_stamp, 'year': year_, 'month': month_, 'day': day}

    # All available parameter values
    all_param_values = dict(src_file_param_values, **time_param_values)

    return [config['dest_template'].format(**all_param_values)]


def get_expected_list(product_config, product_name, year=None, month=None):
    """
    What is the expected list of files in AWS for a given product for given (optinal) year and time
    """

    items_all = get_indexed_info(product_name, year, month)
    with Datacube(app='aws_check') as dc:
        bands = dc.index.products.get_by_name(product_name).measurements.items()
    file_names = set()
    for uuid, filename, dataset_time in items_all:
        prefixes = get_prefixes(filename, dataset_time, product_config.cfg)
        aws_dir = product_config.cfg['aws_dir']
        aws_object_prefix = f'{aws_dir}/{product_config.aws_dir_suffix(prefixes[0])}'
        file_names.update(
            {(str(uuid), f'{aws_object_prefix}/{prefixes[0] + "_" + band[0] + ".tif"}') for band in bands}
        )
        file_names.add((str(uuid), f'{aws_object_prefix}/{prefixes[0] + ".yaml"}'))

    return file_names


def has_x_y(config):
    """
    Does this product config has x, y params in AWS directory structure
    """
    aws_dir_suffix_template = config.cfg['aws_dir_suffix']
    aws_dir_params = compile(aws_dir_suffix_template)._named_fields
    return 'x' in aws_dir_params and 'y' in aws_dir_params


def aws_search_template(config, year=None, month=None):
    """
    What is the prefix template that can be used for a AWS s3 object for a given product
    for given (optional) year and month
    """

    aws_dir_suffix_template = config.cfg['aws_dir_suffix']
    aws_dir_params = compile(aws_dir_suffix_template)._named_fields
    if 'x' in aws_dir_params and 'y' in aws_dir_params:
        if month and year and 'month' in aws_dir_params:
            date_str = datetime(year=year, month=month, day=1).strftime('%Y%m')
            year_ = date_str[0:4]
            month_ = date_str[4:6]
            prefix_template = 'x_{x}/y_{y}/' + year_ + '/' + month_ + '/'
        elif year and 'year' in aws_dir_params:
            date_str = datetime(year=year, month=1, day=1).strftime('%Y')
            year_ = date_str[0:4]
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
    """
    Get all s3 objects for given prefix and kwargs and connection parameters
    """
    kwargs_ = kwargs.copy()
    aws_object_list = set()
    while True:
        resp = connection.list_objects_v2(**kwargs_, Prefix=prefix)
        if resp.get('KeyCount'):
            aws_object_list.update({item['Key'] for item in resp.get('Contents')})
        try:
            kwargs_['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return aws_object_list


def get_aws_list(config, bucket, year=None, month=None):
    """
    What is the list of s3 objects that is present in AWS for a given product for given (optional) year and time
    """
    # session = boto3.Session(profile_name='prod')
    # conn = session.client('s3')
    conn = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    top_level_aws_dir = config.cfg['aws_dir']
    if not top_level_aws_dir[-1] == '/':
        top_level_aws_dir += '/'
    aws_object_list = set()
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
            aws_object_list.update(get_all_s3_with_prefix(conn, kwargs, search_prefix_template.format(x=x, y=y)))
        return aws_object_list
    else:
        return get_all_s3_with_prefix(conn, kwargs, search_prefix_template)


def compare_nci_with_aws(cfg, product_name, year, month, bucket):
    """
    Compare s3 objects present in AWS with what is expected
    """
    config = COGProductConfiguration(cfg['products'][product_name])
    aws_set = get_aws_list(config, bucket, year, month)
    expected_set = get_expected_list(config, product_name, year, month)
    expected_set_no_uuid = {item for _, item in expected_set}
    aws_not_nci = {item for item in aws_set if item not in expected_set_no_uuid}
    nci_not_aws = {f'{uuid}:{item}' for uuid, item in expected_set if item not in aws_set}
    return {'aws_but_not_nci': aws_not_nci, 'nci_but_not_aws': nci_not_aws}


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--bucket', '-b', required=True, type=click.Path(), help="AWS bucket")
def check_nci_with_s3(config, product_name, year, month, bucket):
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)
    result = compare_nci_with_aws(cfg, product_name, year, month, bucket)
    for item in result['aws_but_not_nci']:
        print(f'excess-aws-file:{item}')
    for item in result['nci_but_not_aws']:
        print(f'missing-in-aws:{item}')


if __name__ == '__main__':
    cli()

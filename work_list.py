import re
from datetime import datetime
from pathlib import Path

import click
import yaml
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory
from pandas import Timestamp

from datacube import Datacube
from datacube.model import Range

with open('aws_products_config.yaml', 'r') as fd:
    CFG = yaml.load(fd)


def check_date(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        return Timestamp(value)
    except ValueError as error:
        raise ValueError('Date must be valid string for pandas Timestamp') from error


@click.command(help=__doc__)
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--from-date', callback=check_date, help="The date from which the dataset time")
@click.option('--output_dir', '-o', help='The list will be saved to this directory')
@click.option('--inventory-manifest', '-i',
              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
              help="The manifest of AWS inventory list")
def generate_work_list(product_name, year, month, from_date, output_dir, inventory_manifest):
    """
    Connect to an ODC database and list datasets
    """

    # Filter the inventory list to obtain yaml files corresponding to the product
    s3_client = make_s3_client()
    s3_yaml_keys = set(yaml_files_for_product(list_inventory(inventory_manifest, s3=s3_client), product_name))

    # We only want to process datasets that are not in AWS bucket
    uris = [uri
            for uri, prefix in get_dataset_values(product_name, year, month, from_date, 'dea-prod')
            if prefix + '.yaml' not in s3_yaml_keys]

    out_file = Path(output_dir) / 'file_list'
    with open(out_file, 'w') as fp:
        for item in sorted(uris):
            fp.write(item + '\n')


def get_dataset_values(product, year=None, month=None, from_date=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """

    query = {'product': product}
    if from_date:
        query['time'] = Range(datetime(year=from_date.year, month=from_date.month, day=from_date.day),
                              datetime.now())
    elif year and month:
        query['time'] = Range(datetime(year=year, month=month, day=1), datetime(year=year, month=month + 1, day=1))
    elif year:
        query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    dc = Datacube(app='cog-worklist query', env=datacube_env)

    field_names = get_field_names(product)
    files = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)

    # Extract file name from search_result
    def filename_from_uri(uri):
        return uri.split('//')[1]

    for result in files:
        yield filename_from_uri(result.uri), compute_prefix_from_query_result(result, product)


def yaml_files_for_product(inventory_list, product):
    """
    Filter the given list of s3_keys to yield '.yaml' files corresponding to a product
    """

    for item in inventory_list:
        if item.Key.startswith(CFG['products'][product]['prefix']) and Path(item.Key).suffix == '.yaml':
            yield item.Key


def get_field_names(product_config):
    """
    Get field names for a datacube query for a given product
    """

    # Get parameter names
    param_names = get_param_names(product_config['name_template'])

    # Populate field names
    field_names = ['uri']
    if 'x' in param_names or 'y' in param_names:
        field_names.append('metadata_doc')
    if 'time' in param_names or 'start_time' in param_names or 'end_time' in param_names:
        field_names.append('time')
    if 'lat' in param_names:
        field_names.append('lat')
    if 'lon' in param_names:
        field_names.append('lon')
    return field_names


def get_param_names(template_str):
    """
    Return parameter names from a template string
    """
    return list(set(re.findall(r'{([\w_]+)[:Ymd\-%]*}', template_str)))


def compute_prefix_from_query_result(result, product_config):
    """
    Compute the AWS prefix for a dataset from a datacube search result
    """
    params = {}

    # Get geo x and y values
    if hasattr(result, 'metadata_doc'):
        metadata = result.metadata_doc
        geo_ref = metadata['grid_spatial']['projection']['geo_ref_points']['ll']
        params['x'] = int(geo_ref['x'] / 100000)
        params['y'] = int(geo_ref['y'] / 100000)

    # Get lat and lon values
    if hasattr(result, 'lat'):
        params['lat'] = result.lat
    if hasattr(result, 'lon'):
        params['lon'] = result.lon

    # Compute time values
    if hasattr(result, 'time'):
        mid_time = result.time.lower + (result.time.upper - result.time.lower) / 2
        params['time'] = mid_time
        params['start_time'] = result.time.lower
        params['end_time'] = result.time.upper

    return product_config['prefix'] + product_config['name_template'].format(**params)


if __name__ == '__main__':
    generate_work_list()

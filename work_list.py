import click
from datacube import Datacube
from datacube.model import Range
from dea.aws import make_s3_client
from dea.aws.inventory import list_inventory

from datetime import datetime
from pandas import Timestamp
from pathlib import Path
from parse import parse
import yaml
from parse import compile

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


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
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
    Connect to an ODC database and list NetCDF files
    """

    # Filter the inventory list to obtain yaml files corresponding to the product
    s3_client = make_s3_client()
    s3_yaml_keys = list(yaml_files_for_product(list_inventory(inventory_manifest, s3=s3_client), product_name))

    import ipdb; ipdb.set_trace()
    # We only want to process datasets that are not in AWS bucket
    uris = [uri for uri, prefix in get_dataset_values(product_name, year, month, from_date, 'dea-prod')
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
    dc = Datacube(app='streamer', env=datacube_env)

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
        if parse(CFG['products'][product]['prefix'] + '/{}.yaml', item.Key):
            yield item.Key


def get_field_names(product):
    """
    Get field names for a datacube query for a given product
    """
    param_names = compile(CFG['products'][product]['prefix'])._named_fields
    field_names = ['uri']
    if 'x' in param_names or 'y' in param_names:
        field_names.append('metadata_doc')
    if 'year' in param_names or 'month' in param_names or 'day' in param_names:
        field_names.append('time')
    if 'lat' in param_names:
        field_names.append('lat')
    if 'lon' in param_names:
        field_names.append('lon')
    return field_names


def compute_prefix_from_query_result(result, product):
    """
    Compute the AWS prefix corresponding to a dataset from datacube search result
    """

    param_names = compile(CFG['products'][product]['prefix'])._named_fields

    params = {}

    # Get geo x and y values
    if hasattr(result, 'metadata_doc'):
        metadata = result.metadata_doc
        geo_ref = metadata['grid_spatial']['projection']['geo_ref_points']['ll']
        params['x'], params['y'] = int(geo_ref['x'] / 100000), int(geo_ref['y'] / 100000)

    # Get lat and lon values
    if hasattr(result, 'lat'):
        params['lat'] = result.lat
    if hasattr(result, 'lon'):
        params['lon'] = result.lon

    # Compute time values
    if hasattr(result, 'time'):
        mid_time = result.time.lower + (result.time.upper - result.time.lower) / 2
        if 'year' in param_names:
            params['year'] = mid_time.year
        if 'month' in param_names:
            params['month'] = mid_time.month
        if 'day' in param_names:
            params['day'] = mid_time.day

    return CFG['products'][product]['prefix'].format(**params)


if __name__ == '__main__':
    cli()

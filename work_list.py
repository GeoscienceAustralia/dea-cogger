import click
from datacube import Datacube
from datacube.model import Range
from datetime import datetime
from pandas import Timestamp
from pathlib import Path
from parse import parse
import yaml
from parse import compile

with open('aws_products_cinfig.yaml', 'r') as fd:
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
def generate_work_list(product_name, year, month, from_date, output_dir):
    """
    Connect to an ODC database and list NetCDF files
    """

    # get file list
    items_all = get_indexed_files(product_name, year, month, from_date, 'dea-prod')

    out_file = Path(output_dir) / 'file_list'
    with open(out_file, 'w') as fp:
        for item in sorted(items_all):
            fp.write(item + '\n')


def get_indexed_files(product, year=None, month=None, from_date=None, datacube_env=None):
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

    params = compile(CFG['products'][product]['prefix'])._named_fields
    field_names = ['uri']
    if 'x' in params or 'y' in params:
        field_names.append('metadata_doc')
    if 'year' in params or 'month' in params or 'day' in params:
        field_names.append('time')
    if 'lat' in params:
        field_names.append('lat')
    if 'lon' in params:
        field_names.append('lon')
    files = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)

    import ipdb; ipdb.set_trace()
    # Extract file name from search_result
    def filename_from_uri(uri):
        return uri.split('//')[1]

    for result in files:

        # Get geo x and y values
        metadata = result[2]
        geo_ref = metadata['grid_spatial']['projection']['geo_ref_points']['ll']
        ref_x, ref_y = int(geo_ref['x']/100000), int(geo_ref['y']/100000)
        yield filename_from_uri(result[0])


if __name__ == '__main__':
    cli()

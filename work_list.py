import click
from datacube import Datacube
from datacube.model import Range
from datetime import datetime
from pandas import Timestamp
from pathlib import Path


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
    files = dc.index.datasets.search_returning(field_names=('uri',), **query)

    # Extract file name from search_result
    def filename_from_uri(uri):
        return uri[0].split('//')[1]

    return set(filename_from_uri(uri) for uri in files)


if __name__ == '__main__':
    cli()

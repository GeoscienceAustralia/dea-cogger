import os
import re
import sys
from datetime import timezone

import click
import dateutil.parser
import structlog
from datacube import Datacube
from datacube.ui import parse_expressions

from dea_cogger.cogeo import NetCDFCOGConverter

LOG = structlog.get_logger()


def get_dataset_values(product_name, product_config, time_range=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    try:
        query = {**dict(product=product_name), **time_range}
    except TypeError:
        # Time range is None
        query = {**dict(product=product_name)}

    dc = Datacube(app='cog-worklist query')

#    field_names = get_field_names(product_config)

#    LOG.info(f"Perform a datacube dataset search returning only the specified fields, {field_names}.")
#    ds_records = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)
    ds_records = dc.index.datasets.search(**query)

    search_results = False
    for ds_rec in ds_records:
        search_results = True
        if len(ds_rec.uris) == 1:
            uri = ds_rec.uris[0]
        else:
            uri = [uri for uri in ds_rec.uris if '#part=' in uri][0]
        yield uri, filename_prefix_from_dataset(ds_rec, product_config)

    if not search_results:
        LOG.warning(f"Datacube product query is empty for {product_name} product with time-range, {time_range}")


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


def validate_time_range(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        parse_expressions(value)
        return value
    except SyntaxError:
        raise click.BadParameter('Date range must be in one of the following format as a string:'
                                 "\n\t'2018-01-01 < time < 2018-12-31'  OR\n"
                                 "\n\t'time in 2018-12-31'  OR\n"
                                 "\n\t'time=2018-12-31'")


def _convert_cog(product_config, in_filepath, output_prefix):
    """
    Convert a NetCDF file into a set of Cloud Optimise GeoTIFF files

    Uses a configuration dictionary to define the file naming schema.
    """
    convert_to_cog = NetCDFCOGConverter(**product_config)
    convert_to_cog(in_filepath, output_prefix)


def get_param_names(template_str):
    """
    Return parameter names from a template string

    Use the product template string from the `aws_products_config.yaml` file and return
    unique occurrence of the keys within the `name_template`

    ex. Let name_template = x_{x}/y_{y}/{time:%Y}/{time:%m}/{time:%d}/LS_WATER_3577_{x}_{y}_{time:%Y%m%d%H%M%S%f}
        A regular expression on `name_template` would return `['x', 'y', 'time', 'time', 'time', 'x', 'y']`
        Hence, set operation on the return value will give us unique values of the list.
    """
    # Greedy match any word within curly braces and match a single character from the list [:YmdHMSf%]
    return set(re.findall(r'{([\w]+)[:YmdHMSf%]*}', template_str))


def expected_bands(product_name):
    dc = Datacube(app='cog-worklist query')
    prod = dc.index.products.get_by_name(product_name)
    available_measurements = set(prod.measurements.keys())
    # TODO: Implement black and white listing
    # Actually, maybe delete references to that since I don't believe it's used
    return available_measurements


def filename_prefix_from_dataset(result, product_config):
    """
    Compute the AWS prefix for a dataset from a datacube search result
    """
    params = {}

    # Get geo x and y values
    if hasattr(result, 'metadata_doc'):
        metadata = result.metadata_doc
        try:
            # Process level2 scenes
            satellite_ref_point_start = metadata['image']['satellite_ref_point_start']
            params['x'] = f"{satellite_ref_point_start['x']:03}"
            params['y'] = f"{satellite_ref_point_start['y']:03}"
        except KeyError:
            # Found netCDF files. Process them.
            geo_ref = metadata['grid_spatial']['projection']['geo_ref_points']['ll']
            params['x'] = int(geo_ref['x'] / 100000)
            params['y'] = int(geo_ref['y'] / 100000)
        params['time'] = dateutil.parser.parse(metadata['extent']['center_dt'])

    # Get lat and lon values
    if hasattr(result, 'lat'):
        params['lat'] = result.lat
    if hasattr(result, 'lon'):
        params['lon'] = result.lon

    # Compute time values
    if hasattr(result, 'time'):
        try:
            # First, try a psycopg2._range.DateTimeTZRange object
            params['start_time'] = result.time.lower.astimezone(timezone.utc)
            params['end_time'] = result.time.upper.astimezone(timezone.utc)
        except AttributeError:
            # Failing that, try a datacube Range object
            params['start_time'] = result.time.begin
            params['end_time'] = result.time.end

    basename = product_config['prefix'] + '/' + product_config['name_template'].format(**params)
    return basename


def _mpi_init():
    """
    Ensure we're running within a good MPI environment, and find out the number of processes we have.
    """
    from mpi4py import MPI
    job_rank = MPI.COMM_WORLD.rank  # Rank of this process
    job_size = MPI.COMM_WORLD.size  # Total number of processes
    universe_size = MPI.COMM_WORLD.Get_attr(MPI.UNIVERSE_SIZE)
    pbs_ncpus = os.environ.get('PBS_NCPUS', None)
    if pbs_ncpus is not None and int(universe_size) != int(pbs_ncpus):
        LOG.error('Running within PBS and not using all available resources. Abort!')
        sys.exit(1)
    LOG.info('MPI Info', mpi_job_size=job_size, mpi_universe_size=universe_size, pbs_ncpus=pbs_ncpus)
    return job_rank, job_size


def nth_by_mpi(iterator):
    """
    Use to split an iterator based on MPI pool size and rank of this process
    """
    from mpi4py import MPI
    job_size = MPI.COMM_WORLD.size  # Total number of processes
    job_rank = MPI.COMM_WORLD.rank  # Rank of this process
    for i, element in enumerate(iterator):
        if i % job_size == job_rank:
            yield element

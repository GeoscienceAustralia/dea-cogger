#!/usr/bin/env python3
import csv
import os
import pickle
import re
import shutil
import socket
import subprocess
import sys
from enum import IntEnum
from functools import partial
from os.path import split, basename
from pathlib import Path

import click
import dateutil.parser
import digitalearthau
import structlog
import yaml
from aws_inventory import list_inventory
from aws_s3_client import make_s3_client
from cogeo import NetCDFCOGConverter
from datacube import Datacube
from datacube.ui.expression import parse_expressions
from tqdm import tqdm

LOG = structlog.get_logger()

ROOT_DIR = Path(__file__).absolute().parent
COG_FILE_PATH = ROOT_DIR / 'cog_conv_app.py'
YAMLFILE_PATH = ROOT_DIR / 'aws_products_config.yaml'
GENERATE_FILE_PATH = ROOT_DIR / 'generate_work_list.sh'
AWS_SYNC_PATH = ROOT_DIR / 'aws_sync.sh'
VALIDATE_GEOTIFF_CMD = ROOT_DIR.parent / 'validate_cloud_optimized_geotiff.py'
PICKLE_FILE_EXT = '_s3_inv_list.pickle'
TASK_FILE_EXT = '_file_list.txt'

# pylint: disable=invalid-name
queue_option = click.option('--queue', '-q', default='normal',
                            type=click.Choice(['normal', 'express']))

# pylint: disable=invalid-name
project_option = click.option('--project', '-P', default='v10')

# pylint: disable=invalid-name
output_dir_option = click.option('--output-dir', '-o', required=True,
                                 type=click.Path(exists=True, writable=True),
                                 help='Output destination directory')

# pylint: disable=invalid-name
product_option = click.option('--product-name', '-p', required=True,
                              help="Product name as defined in product configuration file")

# pylint: disable=invalid-name
s3_output_dir_option = click.option('--s3-output-url', default=None, required=True,
                                    help="S3 URL for uploading the converted files")

# pylint: disable=invalid-name
s3_inv_option = click.option('--inventory-manifest', '-i',
                             default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                             help="The manifest of AWS S3 bucket inventory URL")

# pylint: disable=invalid-name
aws_profile_option = click.option('--aws-profile', default='default', help='AWS profile name')

# pylint: disable=invalid-name
s3_pickle_file_option = click.option('--pickle-file', default=None,
                                     help='Pickle file containing the list of s3 bucket inventory')

# pylint: disable=invalid-name
config_file_option = click.option('--config', '-c', default=YAMLFILE_PATH,
                                  help='Product configuration file')

# pylint: disable=invalid-name
# https://cs.anu.edu.au/courses/distMemHPC/sessions/MF1.html
num_nodes_option = click.option('--nodes', default=5,
                                help='Number of raijin nodes (range: 1-3592) to request',
                                type=click.IntRange(1, 3592))

# pylint: disable=invalid-name
walltime_option = click.option('--walltime', '-t', default=10,
                               help='Number of hours (range: 1-48hrs) to request',
                               type=click.IntRange(1, 48))

# pylint: disable=invalid-name
mail_option = click.option('--email-options', '-m', default='abe',
                           type=click.Choice(['a', 'b', 'e', 'n', 'ae', 'ab', 'be', 'abe']),
                           help='Send email when execution is, \n'
                                '[a = aborted | b = begins | e = ends | n = do not send email]')

# pylint: disable=invalid-name
mail_id_option = click.option('--email-id', '-M', default='nci.monitor@dea.ga.gov.au',
                              help='Email recipient id')

# pylint: disable=invalid-name
sat_row_options = click.option('--sat-row', default=0, type=click.INT,
                               help='Image satellite row (Optional)')

# pylint: disable=invalid-name
sat_path_options = click.option('--sat-path', default=0, type=click.INT,
                                help='Image satellite path (Optional)')


class MPITagStatus(IntEnum):
    """
    MPI message tag status
    """
    READY = 1
    START = 2
    DONE = 3
    EXIT = 4
    ERROR = 5


def _submit_qsub_job(command):
    # TODO WTF
    try:
        LOG.info('Running command: %s', command)
        proc_output = subprocess.run(command, shell=True, check=True,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     universal_newlines=True,
                                     encoding='utf-8',
                                     errors='replace')

        for line in proc_output.stdout.split(os.linesep):
            try:
                log_value = line.encode('ascii').decode('utf-8')
                if '.r-man2' in log_value:
                    return log_value
            except UnicodeEncodeError:
                pass
        LOG.error("No qsub job submitted, hence exiting")
        sys.exit(1)
    except subprocess.CalledProcessError as suberror:
        for line in suberror.stdout.split(os.linesep):
            try:
                log_value = line.encode('ascii').decode('utf-8')
                LOG.error(log_value)
            except UnicodeEncodeError:
                LOG.warning("UnicodeEncodeError : %s", line.encode('ascii', 'replace'))
        LOG.error("Subprocess call error, hence exiting")
        raise


def _convert_cog(product_config, in_filepath, output_prefix):
    """
    Convert a NetCDF file into a set of Cloud Optimise GeoTIFF files

    Uses a configuration dictionary to define the file naming schema.
    """
    convert_to_cog = NetCDFCOGConverter(**product_config)
    convert_to_cog(in_filepath, output_prefix)


def validate_time_range(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        parse_expressions(value)
        return value
    except SyntaxError:
        raise click.BadParameter('Date range must be in one of the following format as a string:'
                                 '\n\t1996-01 < time < 1996-12'
                                 '\n\t1996 < time < 1997'
                                 '\n\t1996-01-01 < time < 1996-12-31'
                                 '\n\ttime in 1996'
                                 '\n\ttime in 1996-12'
                                 '\n\ttime in 1996-12-31'
                                 '\n\ttime=1996'
                                 '\n\ttime=1996-12'
                                 '\n\ttime=1996-12-31')


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

    field_names = get_field_names(product_config)

    LOG.info(f"Perform a datacube dataset search returning only the specified fields, {field_names}.")
    ds_records = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)

    search_results = False
    for ds_rec in ds_records:
        search_results = True
        yield check_prefix_from_query_result(ds_rec, product_config)

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


def check_prefix_from_query_result(result, product_config):
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
        params['start_time'] = result.time.lower
        params['end_time'] = result.time.upper

    cog_file_uri_prefix = product_config['prefix'] + '/' + product_config['name_template'].format(**params)
    new_s3_yamlfile = split(cog_file_uri_prefix)[0] + '/' + basename(result.uri).split('.')[0] + '.yaml'
    return result.uri, product_config['name_template'].format(**params), new_s3_yamlfile


# pylint: disable=invalid-name
time_range_options = click.option('--time-range', callback=validate_time_range, required=True,
                                  help="The time range:\n"
                                       " '2018-01-01 < time < 2018-12-31'  OR\n"
                                       " 'time in 2018-12-31'  OR\n"
                                       " 'time=2018-12-31'")


@click.group(help=__doc__,
             context_settings=dict(max_content_width=200))  # Will still shrink to screen width
def cli():
    from tqdm import tqdm

    # See https://github.com/tqdm/tqdm/issues/313
    hostname = socket.gethostname()
    proc_id = os.getpid()

    def add_proc_info(_, logger, event_dict):
        event_dict["hostname"] = hostname
        event_dict["pid"] = proc_id
        return event_dict

    from mpi4py import MPI
    mpi_rank = MPI.COMM_WORLD.rank  # Rank of this process
    mpi_size = MPI.COMM_WORLD.size  # Rank of this process

    def add_mpi_rank(_, logger, event_dict):
        if mpi_size > 1:
            event_dict['mpi_rank'] = mpi_rank
        return event_dict

    def tqdm_logger_factory():
        return TQDMLogger()

    class TQDMLogger:
        def msg(self, message):
            tqdm.write(message)

        log = debug = info = warm = warning = msg
        fatal = failure = err = error = critical = exception = msg

    running_interactively = sys.stdout.isatty() and os.environ.get('PBS_NCPUS', None) is None

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            add_proc_info,
            add_mpi_rank,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer() if running_interactively else structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=tqdm_logger_factory if running_interactively else structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


@cli.command(name='convert', help="Convert a single/list of files into COG format")
@product_option
@config_file_option
@output_dir_option
@click.option('--filelist', '-l', help='List of input file names', default=None)
@click.argument('filenames', nargs=-1, type=click.Path())
def convert(product_name, config, output_dir, filelist, filenames):
    """
    Convert a single or list of input files into Cloud Optimise GeoTIFF format
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    with open(config) as config_file:
        CFG = yaml.load(config_file)

    product_config = CFG['products'][product_name]

    try:
        with open(filelist) as fl:
            reader = csv.reader(fl)
            tasks = list(reader)
    except FileNotFoundError:
        LOG.error('No input file for the COG conversion found in the specified path')
        sys.exit(1)
    if len(tasks) == 0:
        LOG.warning('Task file is empty')
        sys.exit(1)

    cog_converter = NetCDFCOGConverter(**product_config)
    LOG.info("Running with single process")

    for in_filepath, out_prefix in tasks:
        cog_converter(in_filepath, Path(output_dir) / out_prefix)


@cli.command(name='save-s3-inventory', help="Save S3 inventory list in a pickle file")
@product_option
@config_file_option
@output_dir_option
@s3_inv_option
@aws_profile_option
def save_s3_inventory(product_name, config, output_dir, inventory_manifest, aws_profile):
    """
    Scan through S3 bucket for the specified product and fetch the file path of the uploaded files.

    Save those file into a pickle file for further processing.
    Uses a configuration file to define the file naming schema.
    \b
    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    s3_inv_list_file = set()
    with open(config) as config_file:
        config = yaml.load(config_file)

    for result in list_inventory(inventory_manifest, s3=make_s3_client(profile=aws_profile), aws_profile=aws_profile):
        # Get the list for the product we are interested
        if config['products'][product_name]['prefix'] in result.Key and \
                Path(result.Key).name.endswith('.yaml'):
            # Store only metadata configuration files for `set` operation
            s3_inv_list_file.add(result.Key)

    with open(Path(output_dir) / (product_name + PICKLE_FILE_EXT), "wb") as pickle_out:
        pickle.dump(s3_inv_list_file, pickle_out)


@cli.command(name='generate-work-list', help="Generate task list for COG conversion")
@product_option
@time_range_options
@config_file_option
@output_dir_option
@s3_pickle_file_option
def generate_work_list(product_name, time_range, config, output_dir, pickle_file):
    """
    Compares datacube file uri's against S3 bucket (file names within pickle file) and writes the list of datasets
    for cog conversion into the task file
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    with open(config) as config_file:
        config = yaml.load(config_file)

    if pickle_file is None:
        # Use pickle file generated by save_s3_inventory function
        pickle_file = Path(output_dir) / (product_name + PICKLE_FILE_EXT)

    with open(pickle_file, "rb") as pickle_in_fl:
        s3_file_list = pickle.load(pickle_in_fl)

    dc_workgen_list = dict()

    for uri, dest_dir, dc_yamlfile_path in get_dataset_values(product_name,
                                                              config['products'][product_name],
                                                              parse_expressions(time_range)):
        if uri:
            dc_workgen_list[dc_yamlfile_path] = (uri.split('file://')[1], dest_dir)

    work_list = set(dc_workgen_list.keys()) - set(s3_file_list)
    out_file = Path(output_dir) / (product_name + TASK_FILE_EXT)

    with open(out_file, 'w', newline='') as fp:
        csv_writer = csv.writer(fp, quoting=csv.QUOTE_MINIMAL)
        for s3_filepath in work_list:
            # dict_value shall contain uri value and s3 output directory path template
            input_file, dest_dir = dc_workgen_list.get(s3_filepath, (None, None))
            if input_file:
                LOG.info(f"File does not exists in S3, add to processing list: {input_file}")
                csv_writer.writerow((input_file, dest_dir))

    if not work_list:
        LOG.info(f"No tasks found")


@cli.command(name='mpi-convert', help="Bulk COG conversion using MPI")
@product_option
@config_file_option
@output_dir_option
@click.argument('filelist', nargs=1, required=True)
def mpi_convert(product_name, config, output_dir, filelist):
    job_rank, job_size = _mpi_init()

    with open(config) as cfg_file:
        config = yaml.load(cfg_file)

    try:
        with open(filelist) as fl:
            reader = csv.reader(fl)
            tasks = list(reader)
    except FileNotFoundError:
        LOG.error('Task file not found.', filepath=filelist)
        sys.exit(1)

    if len(tasks) == 0:
        LOG.warning('Task file is empty. Aborting.')
        sys.exit(1)

    LOG.info('Successfully loaded configuration file', config=config, taskfile=filelist)

    product_config = config['products'][product_name]

    for i, (in_filepath, s3_dirsuffix) in enumerate(tasks):
        if i % job_size == job_rank:
            try:
                _convert_cog(product_config, in_filepath,
                             Path(output_dir) / s3_dirsuffix.strip())
                LOG.info(f'Successfully converted', filepath=in_filepath)
            except Exception:
                LOG.exception('Unable to convert', filepath=in_filepath)


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


def _nth_by_mpi(iter):
    """
    Use to split an iterator based on MPI pool size and rank of this process
    """
    from mpi4py import MPI
    job_size = MPI.COMM_WORLD.size  # Total number of processes
    job_rank = MPI.COMM_WORLD.rank  # Rank of this process
    for i, element in enumerate(iter):
        if i % job_size == job_rank:
            yield element


@cli.command(name='verify',
             help="Verify GeoTIFFs are Cloud Optimised GeoTIFF")
@click.argument('path', type=click.Path(exists=True))
@click.option('--rm-broken', type=bool, default=False, is_flag=True,
              help="Remove directories with broken files")
def verify(path, rm_broken):
    """
    Verify converted GeoTIFF files are (Geo)TIFF with cloud optimized compatible structure.

    Delete any directories (and their content) that contain broken GeoTIFFs

    :param path: Cog Converted files directory path, or a file containing a list of files to check
    :return:
    """
    job_rank, job_size = _mpi_init()
    broken_files = set()

    path = Path(path)

    if path.is_dir():
        # Non-lazy recursive search for geotiffs
        gtiff_file_list = Path(path).rglob("*.tif")
        gtiff_file_list = list(gtiff_file_list)
    else:
        # Read filenames to check from a file
        with path.open() as fin:
            gtiff_file_list = [line.strip() for line in fin]

    if job_size == 1 and sys.stdout.isatty():
        # Not running in parallel, display a TQDM progress bar
        iter_wrapper = partial(tqdm, disable=None, desc='Checked GeoTIFFs', unit='file')
    elif job_size == 1:
        iter_wrapper = iter
    else:
        # Running in parallel, only process every nth file
        iter_wrapper = _nth_by_mpi

    for geotiff_file in iter_wrapper(gtiff_file_list):
        command = f"python3 {VALIDATE_GEOTIFF_CMD} {geotiff_file}"
        exitcode, output = subprocess.getstatusoutput(command)
        validator_output = output.split('/')[-1]

        if exitcode == 0:
            LOG.debug(validator_output)
        else:
            # Log and remember broken COG
            LOG.info("Invalid GeoTIFF file", error=validator_output, filename=geotiff_file)
            broken_files.add(geotiff_file)

    if rm_broken:
        # Delete directories containing broken files
        broken_directories = set(file.parent for file in broken_files)

        for directory in broken_directories:
            LOG.info('Deleting directory', dir=directory)
            shutil.rmdir(directory)


@cli.command(name='qsub', help="Kick off four stage COG Conversion PBS job")
@product_option
@time_range_options
@config_file_option
@output_dir_option
@queue_option
@project_option
@walltime_option
@num_nodes_option
@mail_option
@mail_id_option
@s3_inv_option
@s3_output_dir_option
@aws_profile_option
@sat_row_options
@sat_path_options
def qsub(product_name, time_range, config, output_dir, queue, project, walltime, nodes,
         email_options, email_id, inventory_manifest, s3_output_url, aws_profile,
         sat_row, sat_path):
    """
    Submits an COG conversion job, using a four stage PBS job submission.
    Uses a configuration file to define the file naming schema.

    Stage 1 (Store S3 inventory list to a pickle file):
        1) Scan through S3 inventory list and fetch the uploaded file names of the desired product
        2) Save those file names in a pickle file

    Stage 2 (Generate work list for COG conversion):
           1) Compares datacube file uri's against S3 bucket (file names within pickle file)
           2) Write the list of datasets not found in S3 to the task file
           3) Repeat until all the datasets are compared against those found in S3

    Stage 3 (COG convert using MPI runs):
           1) Master fetches the file list from the task file
           2) Master shall then assign tasks to worker with task status as 'READY' for cog conversion
           3) Worker executes COG conversion algorithm and sends task status as 'START'
           4) Once worker finishes COG conversion, it sends task status as 'DONE' to the master
           5) If master has more work, then process continues as defined in steps 2-4
           6) If no tasks are pending with master, worker sends task status as 'EXIT' and closes the communication
           7) Finally master closes the communication

    Stage 4 (Validate GeoTIFF files and run AWS sync to upload files to AWS S3):
            1) Validate GeoTIFF files and if valid then upload to S3 bucket
            2) Using aws sync command line tool, sync newly COG converted files to S3 bucket

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    with open(ROOT_DIR / 'aws_products_config.yaml') as fd:
        CFG = yaml.load(fd)
    task_file = str(Path(output_dir) / product_name) + TASK_FILE_EXT

    # Make output directory specific to the a product (if it does not exists)
    (Path(output_dir) / product_name).mkdir(parents=True, exist_ok=True)

    # language=bash
    prep = 'qsub -q copyq -N save_s3_list_%(product)s -P %(project)s ' \
           '-l wd,walltime=10:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           'module use /g/data/v10/public/modules/modulefiles/; ' \
           'module load dea; ' \
           'python3 %(cog_converter_file)s save-s3-inventory -c %(yaml_file)s ' \
           '--inventory-manifest %(s3_inventory)s --aws-profile %(aws_profile)s --product-name %(product)s ' \
           '--output-dir %(output_dir)s"'
    cmd = prep % dict(product=product_name, project=project, cog_converter_file=COG_FILE_PATH,
                      yaml_file=config, s3_inventory=inventory_manifest, aws_profile=aws_profile,
                      output_dir=output_dir)

    s3_inv_job = _submit_qsub_job(cmd)

    pickle_file = Path(output_dir) / (product_name + PICKLE_FILE_EXT),
    file_list_job = _submit_qsub_job(
        f'qsub -q {queue} -N generate_work_list_{product_name} -P {project} '
        f'-W depend=afterok:{s3_inv_job}: -l wd,walltime=10:00:00,mem=31GB,jobfs=5GB -W umask=33 '
        f'-- /bin/bash -l -c "source $HOME/.bashrc; '
        f'{GENERATE_FILE_PATH} --dea-module {digitalearthau.MODULE_NAME} --cog-file {COG_FILE_PATH} '
        f'--config-file {config} --product-name {product_name} --output-dir {output_dir} '
        f'--sat-row {sat_row} --sat-path {sat_path} '
        f'--pickle-file {pickle_file} --time-range \'{time_range}\'"')

    # language=bash
    qsub = 'qsub -q %(queue)s -N mpi_cog_convert_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(file_list_job)s: -m %(email_options)s -M %(email_id)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,jobfs=32GB,walltime=%(walltime)d:00:00,wd ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           'module use /g/data/v10/public/modules/modulefiles/; ' \
           'module load dea/20181015; ' \
           'module load openmpi/3.1.2;' \
           'mpirun --tag-output --report-bindings ' \
           'python3 %(cog_converter_file)s mpi-cog-convert ' \
           '-c %(yaml_file)s --output-dir %(output_dir)s --product-name %(product)s %(file_list)s"'
    _RAM_PER_NODE = 62
    _CPUS_PER_NODE = 16
    cmd = qsub % dict(queue=queue,
                      project=project,
                      file_list_job=file_list_job.split('.')[0],
                      email_options=email_options,
                      email_id=email_id,
                      ncpus=nodes * _CPUS_PER_NODE,
                      mem=nodes * _RAM_PER_NODE,
                      walltime=walltime,
                      cog_converter_file=COG_FILE_PATH,
                      yaml_file=config,
                      output_dir=Path(output_dir) / product_name,
                      product=product_name,
                      file_list=task_file)
    cog_conversion_job = _submit_qsub_job(cmd)

    if s3_output_url:
        s3_output = s3_output_url
    else:
        s3_output = 's3://dea-public-data/' + CFG['products'][product_name]['prefix']

    # language=bash
    prep = 'qsub -q copyq -N s3_sync_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(cog_conversion_job)s: ' \
           '-l wd,walltime=5:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           '%(aws_sync_script)s --dea-module %(dea_module)s ' \
           '--s3-dir %(s3_output_url)s --aws-profile %(aws_profile)s ' \
           '--output-dir %(output_dir)s --cog-file %(cog_converter_file)s"'
    cmd = prep % dict(product=product_name, project=project,
                      cog_conversion_job=cog_conversion_job.split('.')[0],
                      aws_sync_script=AWS_SYNC_PATH, dea_module=digitalearthau.MODULE_NAME,
                      s3_output_url=s3_output, cog_converter_file=COG_FILE_PATH, aws_profile=aws_profile,
                      output_dir=Path(output_dir) / product_name)
    _submit_qsub_job(cmd)


if __name__ == '__main__':
    cli()

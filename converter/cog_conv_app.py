#!/usr/bin/env python
import logging
import os
import pickle
import re
import subprocess
import sys
from enum import IntEnum
from pathlib import Path
from os.path import split, basename

import click
import dateutil.parser
import digitalearthau
import numpy as np
import yaml
from mpi4py import MPI

from aws_inventory import list_inventory
from aws_s3_client import make_s3_client
from cogeo import COGConvert
from datacube import Datacube
from datacube.ui.expression import parse_expressions

LOG = logging.getLogger('cog-converter')
stdout_hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d - %(levelname)s] %(message)s')
stdout_hdlr.setFormatter(formatter)
LOG.addHandler(stdout_hdlr)
LOG.setLevel(logging.DEBUG)

MPI_COMM = MPI.COMM_WORLD  # Get MPI communicator object
MPI_JOB_SIZE = MPI_COMM.size  # Total number of processes
MPI_JOB_RANK = MPI_COMM.rank  # Rank of this process
MPI_JOB_STATUS = MPI.Status()  # Get MPI status object
ROOT_DIR = Path(__file__).absolute().parent
COG_FILE_PATH = ROOT_DIR / 'cog_conv_app.py'
YAMLFILE_PATH = ROOT_DIR / 'aws_products_config.yaml'
GENERATE_FILE_PATH = ROOT_DIR / 'generate_work_list.sh'
AWS_SYNC_PATH = ROOT_DIR / 'aws_sync.sh'
VALIDATE_GEOTIFF_PATH = ROOT_DIR.parent / 'validate_cloud_optimized_geotiff.py'
PICKLE_FILE_EXT = '_s3_inv_list.pickle'
TASK_FILE_EXT = '_file_list.txt'

# pylint: disable=invalid-name
queue_options = click.option('--queue', '-q', default='normal',
                             type=click.Choice(['normal', 'express']))

# pylint: disable=invalid-name
project_options = click.option('--project', '-P', default='v10')

# pylint: disable=invalid-name
output_dir_options = click.option('--output-dir', '-o', required=True,
                                  type=click.Path(exists=True, writable=True),
                                  help='Output destination directory')

# pylint: disable=invalid-name
product_options = click.option('--product-name', '-p', required=True,
                               help="Product name as defined in product configuration file")

# pylint: disable=invalid-name
s3_output_dir_options = click.option('--s3-output-url', default=None,
                                     help="S3 URL for uploading the converted files (Required)")

# pylint: disable=invalid-name
dc_env_options = click.option('--datacube-env', '-E', default='datacube', help='Datacube environment (Optional)')

# pylint: disable=invalid-name
s3_inv_options = click.option('--inventory-manifest', '-i',
                              default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                              help="The manifest of AWS S3 bucket inventory URL (Optional)")

# pylint: disable=invalid-name
aws_profile_options = click.option('--aws-profile', default='default', help='AWS profile name (Optional)')

# pylint: disable=invalid-name
s3_pickle_file_options = click.option('--pickle-file', default=None,
                                      help='Pickle file containing the list of s3 bucket inventory (Optional)')

# pylint: disable=invalid-name
config_file_options = click.option('--config', '-c', default=YAMLFILE_PATH,
                                   help='Product configuration file (Optional)')

# pylint: disable=invalid-name
# https://cs.anu.edu.au/courses/distMemHPC/sessions/MF1.html
num_nodes_options = click.option('--nodes', default=5,
                                 help='Number of raijin nodes (range: 1-3592) to request (Optional)',
                                 type=click.IntRange(1, 3592))

# pylint: disable=invalid-name
walltime_options = click.option('--walltime', '-t', default=10,
                                help='Number of hours (range: 1-48hrs) to request (Optional)',
                                type=click.IntRange(1, 48))

# pylint: disable=invalid-name
mail_options = click.option('--email-options', '-m', default='abe',
                            type=click.Choice(['a', 'b', 'e', 'n', 'ae', 'ab', 'be', 'abe']),
                            help='Send email when execution is, \n'
                                 '[a = aborted | b = begins | e = ends | n = do not send email]')

# pylint: disable=invalid-name
mail_id = click.option('--email-id', '-M', default='nci.monitor@dea.ga.gov.au',
                       help='Email recipient id (Optional)')

with open(ROOT_DIR / 'aws_products_config.yaml') as fd:
    CFG = yaml.load(fd)


class TagStatus(IntEnum):
    """
        MPI message tag status
    """
    READY = 1
    START = 2
    DONE = 3
    EXIT = 4
    ERROR = 5


def run_command(command):
    """
    A simple utility to execute a subprocess command.
    """
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


def cog_converter(product_config, in_filepath, output_dir):
    """
    Convert a list of input files into Cloud Optimise GeoTIFF format using MPI
    Uses a configuration file to define the file naming schema.
    """
    convert_to_cog = COGConvert(**product_config)
    convert_to_cog(in_filepath, output_dir.parent)


def _raise_value_err(exp):
    raise ValueError(exp)


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


def get_dataset_values(product_name, time_range=None, datacube_env='datacube'):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    try:
        query = {**dict(product=product_name), **time_range}
    except TypeError:
        # Time range is None
        query = {**dict(product=product_name)}

    dc = Datacube(app='cog-worklist query', env=datacube_env)

    field_names = get_field_names(CFG['products'][product_name])

    LOG.info(f"Perform a datacube dataset search returning only the specified fields, {field_names}.")
    ds_records = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)

    search_results = False
    for ds_rec in ds_records:
        search_results = True
        yield check_prefix_from_query_result(ds_rec, CFG['products'][product_name])

    if not search_results:
        LOG.warning(f"Datacube product query is empty for {product_name} product with time-range, {time_range}")
        return "", "", ""


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
    # Greedy match any word within flower braces and match single character from the list [:YmdHMSf%]
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
            params['x'] = int(satellite_ref_point_start['x'])
            params['y'] = int(satellite_ref_point_start['y'])
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


@click.group(help=__doc__)
def cli():
    pass


@cli.command(name='cog-convert', help="Convert a single/list of files into COG format")
@product_options
@config_file_options
@output_dir_options
@click.option('--filelist', '-l', help='List of input file names (Optional)', default=None)
@click.argument('filenames', nargs=-1, type=click.Path())
def convert_cog(product_name, config, output_dir, filelist, filenames):
    """
    Convert a single or list of input files into Cloud Optimise GeoTIFF format
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    global CFG
    with open(config) as config_file:
        CFG = yaml.load(config_file)

    product_config = CFG['products'][product_name]

    cog_convert = COGConvert(**product_config)

    # Preference is give to file list over argument list
    if filelist:
        try:
            with open(filelist) as fl:
                file_list = np.genfromtxt(fl, dtype='str', delimiter=",")
            tasks = file_list.size
        except FileNotFoundError:
            LOG.error(f'No input file for the COG conversion found in the specified path')
            raise
        else:
            if tasks == 0:
                LOG.warning(f'Task file is empty')
                sys.exit(1)
    else:
        file_list = list(filenames)

    LOG.info("Run with single process")
    try:
        # We have file path and s3 output prefix in the file list
        for in_filepath, _ in file_list:
            cog_convert(in_filepath, Path(output_dir))
    except ValueError:
        # We have only file path in the file list
        for in_filepath in file_list:
            cog_convert(in_filepath, Path(output_dir))


@cli.command(name='save-s3-inventory', help="Save S3 inventory list in a pickle file")
@product_options
@config_file_options
@output_dir_options
@s3_inv_options
@aws_profile_options
def save_s3_inventory(product_name, config, output_dir, inventory_manifest, aws_profile):
    """
    Scan through S3 bucket for the specified product and fetch the file path of the uploaded files.
    Save those file into a pickle file for further processing.
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    s3_inv_list_file = list()
    global CFG
    with open(config) as config_file:
        CFG = yaml.load(config_file)

    for result in list_inventory(inventory_manifest, s3=make_s3_client(profile=aws_profile), aws_profile=aws_profile):
        # Get the list for the product we are interested
        if CFG['products'][product_name]['prefix'] in result.Key and \
           Path(result.Key).name.endswith('.yaml'):
            # Store only metadata configuration files for `set` operation
            s3_inv_list_file.append(result.Key)
    pickle_out = open(Path(output_dir) / (product_name + PICKLE_FILE_EXT), "wb")
    pickle.dump(set(s3_inv_list_file), pickle_out)
    pickle_out.close()


@cli.command(name='generate-work-list', help="Generate task list for COG conversion")
@product_options
@time_range_options
@config_file_options
@output_dir_options
@dc_env_options
@s3_pickle_file_options
def generate_work_list(product_name, time_range, config, output_dir, datacube_env, pickle_file):
    """
    Compares datacube file uri's against S3 bucket (file names within pickle file) and writes the list of datasets
    for cog conversion into the task file
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    global CFG
    with open(config) as config_file:
        CFG = yaml.load(config_file)

    if not pickle_file:
        # Use pickle file generated by save_s3_inventory function
        pickle_in_fl = open(Path(output_dir) / product_name + PICKLE_FILE_EXT, "rb")
        s3_file_list = pickle.load(pickle_in_fl)
    else:
        # Use pickle file supplied by the user
        pickle_in_fl = open(Path(pickle_file), "rb")
        s3_file_list = pickle.load(pickle_in_fl)

    dc_workgen_list = dict()

    for uri, dest_dir, dc_yamlfile_path in get_dataset_values(product_name,
                                                              parse_expressions(time_range),
                                                              datacube_env):
        if uri:
            dc_workgen_list[dc_yamlfile_path] = [uri.split('file://')[1], dest_dir]

    work_list = set(sorted(dc_workgen_list.keys())) - set(sorted(s3_file_list))
    out_file = Path(output_dir) / (product_name + TASK_FILE_EXT)

    with open(out_file, 'w') as fp:
        for s3_filepath in work_list:
            # dict_value shall contain uri value and s3 output directory path template
            dict_value = dc_workgen_list.get(s3_filepath, None)
            if dict_value:
                LOG.info(f"File does not exists in S3, add to work gen list: {dict_value[0]}")
                fp.write(f"{dict_value[0]}, {dict_value[1]}\n")

    if not work_list:
        LOG.info(f"No tasks found")


@cli.command(name='mpi-cog-convert')
@product_options
@config_file_options
@output_dir_options
@click.argument('filelist', nargs=1, required=True)
def mpi_cog_convert(product_name, config, output_dir, filelist):
    """
    \b
    Parallelise COG convert using MPI
    Example: mpirun python3 cog_conv_app.py mpi-cog-convert -c aws_products_config.yaml
             --output-dir /tmp/wofls_cog/ -p wofs_albers /tmp/wofs_albers_file_list
    \f
    Convert input files to COG format using parallelisation by MPI tool.
    Iterate over the file list and assign MPI worker for COG conversion.
    Following details how master and worker interact during MPI cog conversion process:
       1) Master fetches the file list from the task file
       2) Master shall then assign tasks to worker with task status as 'READY' for cog conversion
       3) Worker executes COG conversion algorithm and sends task status as 'START'
       4) Once worker finishes COG conversion, it sends task status as 'DONE' to the master
       5) If master has more work, then process continues as defined in steps 2-4
       6) If no tasks are pending with master, worker sends task status as 'EXIT' and closes the communication
       7) Finally master closes the communication

    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    global MPI_COMM, MPI_JOB_SIZE, MPI_JOB_RANK, CFG

    with open(config) as cfg_file:
        CFG = yaml.load(cfg_file)

    try:
        with open(filelist) as fl:
            file_list = np.genfromtxt(fl, dtype='str', delimiter=",")
        # Divide by 2, since we have two columns (in_filepath and s3_dirsuffix) in a numpy array
        tasks = int(file_list.size / 2)
    except FileNotFoundError:
        LOG.error(f'MPI Worker ({MPI_JOB_RANK}): Task file consisting of file URIs for COG Conversion '
                  f'is not found in the input path')
        raise
    else:
        if tasks == 0:
            LOG.warning(f'MPI Worker ({MPI_JOB_RANK}): Task file is empty')
            sys.exit(1)

    product_config = CFG['products'][product_name]
    num_workers = MPI_JOB_SIZE - 1 if MPI_JOB_SIZE > 1 else _raise_value_err(
        f"MPI Worker ({MPI_JOB_RANK}): Number of required processes has to be greater than 1")

    exit_flag = False
    # Ensure all errors/exceptions are handled before this, else master-worker processes
    # will enter a dead-lock situation
    if MPI_JOB_RANK == 0:
        master_node_name = MPI.Get_processor_name()
        task_index = 0
        closed_workers = 0
        job_args = []
        LOG.debug(f"MPI Master ({MPI_JOB_RANK}) on {master_node_name} node, starting with {num_workers} workers")

        # Append the jobs_args list for each filename to be scheduled among all the available workers
        if tasks == 1:
            job_args = [(product_config, str(file_list), output_dir)]
        elif tasks > 1:
            for in_filepath, s3_dirsuffix in file_list:
                job_args.extend([(product_config, in_filepath, Path(output_dir) / s3_dirsuffix.strip())])

        while closed_workers < num_workers:
            MPI_COMM.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=MPI_JOB_STATUS)
            source = MPI_JOB_STATUS.Get_source()
            tag = MPI_JOB_STATUS.Get_tag()

            if tag == TagStatus.READY:
                # Worker is ready, so assign a task
                if task_index < tasks:
                    MPI_COMM.send(job_args[task_index], dest=source, tag=TagStatus.START)
                    LOG.debug("MPI Master (%d) assigning task to worker (%d): Process %r file" % (MPI_JOB_RANK,
                                                                                                  source,
                                                                                                  job_args[task_index]))
                    task_index += 1
                else:
                    # Completed assigning all the tasks, signal workers to exit
                    MPI_COMM.send((None, None, None), dest=source, tag=TagStatus.EXIT)
            elif tag == TagStatus.DONE:
                LOG.debug(f"MPI Worker ({source}) completed the assigned task")
            elif tag == TagStatus.EXIT:
                LOG.debug(f"MPI Worker ({source}) exited")
                closed_workers += 1
            elif tag == TagStatus.ERROR:
                LOG.debug(f"MPI Worker ({source}) error detected during processing")
                exit_flag = True
                closed_workers += 1

        if exit_flag:
            LOG.error("Error occurred during cog conversion, hence stop everything and do not proceed")
            sys.exit(1)
        else:
            LOG.debug("Batch processing completed")
    else:
        worker_node_name = MPI.Get_processor_name()

        while True:
            # Indicate master that the worker is ready for processing task
            MPI_COMM.send(None, dest=0, tag=TagStatus.READY)

            # Fetch task information from the master
            product_config, in_filepath, out_dir = MPI_COMM.recv(source=0, tag=MPI.ANY_TAG, status=MPI_JOB_STATUS)

            # Get the tag status from the master
            tag = MPI_JOB_STATUS.Get_tag()

            if tag == TagStatus.START:
                LOG.debug(f"MPI Worker ({MPI_JOB_RANK}) on {worker_node_name} node started COG conversion")
                try:
                    cog_converter(product_config, in_filepath, out_dir)
                except Exception as exp:
                    LOG.error(f"Unexpected error occurred during cog conversion: {exp}")
                    MPI_COMM.send(None, dest=0, tag=TagStatus.ERROR)
                    raise
                MPI_COMM.send(None, dest=0, tag=TagStatus.DONE)
            elif tag == TagStatus.EXIT:
                break

        LOG.debug(f"MPI Worker ({MPI_JOB_RANK}) on {worker_node_name} node did not receive any task, hence sending "
                  f"exit status to the master")
        MPI_COMM.send(None, dest=0, tag=TagStatus.EXIT)


@cli.command(name='verify-cog-files',
             help="Verify the converted GeoTIFF are Cloud Optimised GeoTIFF")
@click.option('--path', '-p', required=True, help="Validate the GeoTIFF files from this folder",
              type=click.Path(exists=True))
def verify_cog_files(path):
    """
    Verify converted GeoTIFF files are (Geo)TIFF with cloud optimized compatible structure.

    Mandatory Requirement: `validate_cloud_optimized_geotiff.py` gdal file
    :param path: Cog Converted files directory path
    :return:
    """
    invalid_geotiff_files = list()
    remove_root_geotiff_dir = list()

    gtiff_file_list = sorted(Path(path).rglob("*.tif"))
    count = 0
    for file_path in gtiff_file_list:
        count += 1
        command = f"python3 {VALIDATE_GEOTIFF_PATH} {file_path}"
        output = subprocess.getoutput(command)
        valid_output = output.split('/')[-1]

        if 'is a valid cloud' in valid_output:
            LOG.info(f"{count}: {valid_output}")
        else:
            LOG.error(f"{count}: {valid_output}")

            # List erroneous *.tif file path
            LOG.error(f"\tErroneous GeoTIFF filepath: {file_path}")
            remove_root_geotiff_dir.append(file_path.parent)
            for files in file_path.parent.rglob('*.*'):
                # List all files associated with erroneous *.tif file and remove them
                invalid_geotiff_files.append(files)

    for rm_files in set(invalid_geotiff_files):
        LOG.error(f"\tDelete {rm_files} file")
        rm_files.unlink()

    for rm_dir in set(remove_root_geotiff_dir):
        LOG.error(f"\tDelete {rm_dir} directory")
        rm_dir.rmdir()


@cli.command(name='qsub-cog-convert', help="Kick off four stage COG Conversion PBS job")
@product_options
@time_range_options
@config_file_options
@output_dir_options
@queue_options
@project_options
@walltime_options
@num_nodes_options
@mail_options
@mail_id
@s3_inv_options
@s3_output_dir_options
@aws_profile_options
@dc_env_options
def qsub_cog_convert(product_name, time_range, config, output_dir, queue, project, walltime, nodes,
                     email_options, email_id, inventory_manifest, s3_output_url, aws_profile, datacube_env):
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

    s3_inv_job = run_command(cmd)

    # language=bash
    prep = 'qsub -q %(queue)s -N generate_work_list_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(s3_inv_job)s: -l wd,walltime=10:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           '%(generate_script)s --dea-module %(dea_module)s --cog-file %(cog_converter_file)s ' \
           '--config-file %(yaml_file)s --product-name %(product)s --output-dir %(output_dir)s ' \
           '--datacube-env %(dc_env)s --pickle-file %(pickle_file)s --time-range \'%(time_range)s\'"'
    cmd = prep % dict(queue=queue, product=product_name, project=project, s3_inv_job=s3_inv_job.split('.')[0],
                      generate_script=GENERATE_FILE_PATH, dea_module=digitalearthau.MODULE_NAME,
                      cog_converter_file=COG_FILE_PATH, yaml_file=config, output_dir=output_dir,
                      dc_env=datacube_env, pickle_file=Path(output_dir) / (product_name + PICKLE_FILE_EXT),
                      time_range=time_range)

    file_list_job = run_command(cmd)

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
    cmd = qsub % dict(queue=queue,
                      project=project,
                      file_list_job=file_list_job.split('.')[0],
                      email_options=email_options,
                      email_id=email_id,
                      ncpus=nodes * 16,
                      mem=nodes * 62,
                      walltime=walltime,
                      cog_converter_file=COG_FILE_PATH,
                      yaml_file=config,
                      output_dir=Path(output_dir) / product_name,
                      product=product_name,
                      file_list=task_file)
    cog_conversion_job = run_command(cmd)

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
    run_command(cmd)


if __name__ == '__main__':
    cli()

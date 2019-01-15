#!/usr/bin/env python
import click
import dateutil.parser
import gdal
import logging
import numpy as np
import os
import pickle
import re
import sys
import subprocess
import xarray
import yaml
from datetime import datetime
from enum import IntEnum
from pathlib import Path
from os.path import join as pjoin, basename, exists
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from datacube import Datacube
from datacube.ui.expression import parse_expressions
import digitalearthau
from aws_s3_client import make_s3_client
from aws_inventory import list_inventory
from cogeo import cog_translate
from mpi4py import MPI

LOG = logging.getLogger('cog-converter')
stdout_hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d - %(levelname)s] %(message)s')
stdout_hdlr.setFormatter(formatter)
LOG.addHandler(stdout_hdlr)
LOG.setLevel(logging.DEBUG)

DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}
MPI_COMM = MPI.COMM_WORLD      # Get MPI communicator object
MPI_JOB_SIZE = MPI_COMM.size   # Total number of processes
MPI_JOB_RANK = MPI_COMM.rank   # Rank of this process
MPI_JOB_STATUS = MPI.Status()  # Get MPI status object
ROOT_DIR = Path(__file__).absolute().parent
STREAMER_FILE_PATH = ROOT_DIR / 'streamer.py'
YAMLFILE_PATH = ROOT_DIR / 'aws_products_config.yaml'
GENERATE_FILE_PATH = ROOT_DIR / 'generate_work_list.sh'
AWS_SYNC_PATH = ROOT_DIR / 'aws_sync.sh'
VALIDATE_GEOTIFF_PATH = ROOT_DIR.parent / 'validate_cloud_optimized_geotiff.py'
PICKLE_FILE_EXT = '_s3_inv_list.pickle'
TASK_FILE_EXT = '_nc_file_list.txt'
INVALID_GEOTIFF_FILES = list()
REMOVE_ROOT_GEOTIFF_DIR = list()


# pylint: disable=invalid-name
queue_options = click.option('--queue', '-q', default='normal',
                             type=click.Choice(['normal', 'express']))

# pylint: disable=invalid-name
project_options = click.option('--project', '-P', default='v10')

# pylint: disable=invalid-name
node_options = click.option('--nodes', '-n', default=5,
                            help='Number of nodes to request (Optional)',
                            type=click.IntRange(1, 5))

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


# pylint: disable=invalid-name
output_dir_options = click.option('--output-dir', '-o', default='/g/data/v10/tmp/cog_output_files/',
                                  help='Output work directory (Optional)')

# pylint: disable=invalid-name
product_options = click.option('--product-name', '-p', required=True,
                               help="Product name as defined in product configuration file")

# pylint: disable=invalid-name
dc_env_options = click.option('--datacube-env', '-E', default='datacube', help='Datacube environment (Optional)')

# pylint: disable=invalid-name
s3_options = click.option('--inventory-manifest', '-i',
                          default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                          help="The manifest of AWS S3 bucket inventory URL (Optional)")

# pylint: disable=invalid-name
aws_profile_options = click.option('--aws-profile', default='default', help='AWS profile name (Optional)')

# pylint: disable=invalid-name
s3_inventory_list_options = click.option('--s3-list', default=list(),
                                         help='Pickle file containing the list of s3 bucket inventory (Optional)')

# pylint: disable=invalid-name
config_file_options = click.option('--config', '-c', default=YAMLFILE_PATH,
                                   help='Product configuration file (Optional)')

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


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, nonpym_list=None, default_rsp=None,
                 bands_rsp=None, name_template=None, prefix=None, predictor=None, stacked_name_template=None):
        # A list of keywords of bands which don't require resampling
        self.nonpym_list = nonpym_list

        # A list of keywords of bands excluded in cog convert
        self.black_list = black_list

        # A list of keywords of bands to be converted
        self.white_list = white_list

        self.bands_rsp = bands_rsp
        self.name_template = name_template
        self.prefix = prefix
        self.stacked_name_template = stacked_name_template

        if predictor is None:
            self.predictor = 2
        else:
            self.predictor = predictor

        if default_rsp is None:
            self.default_rsp = 'average'
        else:
            self.default_rsp = default_rsp

    def __call__(self, input_fname, dest_dir):
        prefix_name = self._make_out_prefix(input_fname, dest_dir)
        self.netcdf_to_cog(input_fname, prefix_name)

    def _make_out_prefix(self, input_fname, dest_dir):
        abs_fname = basename(input_fname)
        prefix_name = re.search(r"[-\w\d.]*(?=\.\w)", abs_fname).group(0)
        r = re.compile(r"(?<=_)[-\d.]+")
        indices = r.findall(prefix_name)
        r = re.compile(r"(?<={)\w+")
        keys = sorted(set(r.findall(self.name_template)))

        if len(indices) > len(keys):
            indices = indices[-len(keys):]

        indices += [None] * (3 - len(indices))
        x_index, y_index, date_time = indices

        dest_dict = {keys[1]: x_index, keys[2]: y_index}

        if date_time is not None:
            dest_dict[keys[0]] = datetime.strptime(date_time, '%Y%m%d%H%M%S%f')
        else:
            self.name_template = '/'.join(self.name_template.split('/')[0:2])

        out_dir = Path(pjoin(dest_dir, self.name_template.format(**dest_dict))).parents[0]
        os.makedirs(out_dir, exist_ok=True)

        return pjoin(out_dir, prefix_name)

    def netcdf_to_cog(self, input_file, prefix):
        """
        Convert the datasets in the NetCDF file 'file' into 'dest_dir'

        Each dataset is put in a separate directory.

        The directory names will look like 'LS_WATER_3577_9_-39_20180506102018'
        """
        try:
            dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        except Exception as exp:
            LOG.info(f"netcdf error {input_file}: \n{exp}")
            return

        if dataset is None:
            return

        subdatasets = dataset.GetSubDatasets()

        # Extract each band from the NetCDF and write to individual GeoTIFF files
        rastercount = self._dataset_to_cog(prefix, subdatasets)

        dataset_array = xarray.open_dataset(input_file)
        self._dataset_to_yaml(prefix, dataset_array, rastercount)
        # Clean up XML files from GDAL
        # GDAL creates extra XML files which we don't want

    def _dataset_to_yaml(self, prefix, dataset_array: xarray.Dataset, rastercount):
        """
        Write the datasets to separate yaml files
        """
        for i in range(rastercount):
            if rastercount == 1:
                yaml_fname = prefix + '.yaml'
                dataset_object = (dataset_array.dataset.item()).decode('utf-8')
            else:
                yaml_fname = prefix + '_' + str(i + 1) + '.yaml'
                dataset_object = (dataset_array.dataset.item(i)).decode('utf-8')

            if exists(yaml_fname):
                continue

            dataset = yaml.load(dataset_object, Loader=Loader)
            if dataset is None:
                LOG.info("No yaml section %s", prefix)
                continue

            # Update band urls
            for key, value in dataset['image']['bands'].items():
                if rastercount == 1:
                    tif_path = basename(prefix + '_' + key + '.tif')
                else:
                    tif_path = basename(prefix + '_' + key + '_' + str(i + 1) + '.tif')

                value['layer'] = str(i + 1)
                value['path'] = tif_path

            dataset['format'] = {'name': 'GeoTIFF'}
            dataset['lineage'] = {'source_datasets': {}}
            with open(yaml_fname, 'w') as fp:
                yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)

    def _dataset_to_cog(self, prefix, subdatasets):
        """
        Write the datasets to separate cog files
        """

        os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
        os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'
        if self.white_list is not None:
            self.white_list = "|".join(self.white_list)
        if self.black_list is not None:
            self.black_list = "|".join(self.black_list)
        if self.nonpym_list is not None:
            self.nonpym_list = "|".join(self.nonpym_list)

        rastercount = 0
        for dts in subdatasets[:-1]:
            rastercount = gdal.Open(dts[0]).RasterCount
            for i in range(rastercount):
                band_name = dts[0].split(':')[-1]

                # Only do specified bands if specified
                if self.black_list is not None:
                    if re.search(self.black_list, band_name) is not None:
                        continue

                if self.white_list is not None:
                    if re.search(self.white_list, band_name) is None:
                        continue

                if rastercount == 1:
                    out_fname = prefix + '_' + band_name + '.tif'
                else:
                    out_fname = prefix + '_' + band_name + '_' + str(i + 1) + '.tif'

                # Check the done files might need a force option later
                if exists(out_fname):
                    if self._check_tif(out_fname):
                        continue

                # Resampling method of this band
                resampling_method = None
                if self.bands_rsp is not None:
                    resampling_method = self.bands_rsp.get(band_name)
                if resampling_method is None:
                    resampling_method = self.default_rsp
                if self.nonpym_list is not None:
                    if re.search(self.nonpym_list, band_name) is not None:
                        resampling_method = None

                default_profile = {'driver': 'GTiff',
                                   'interleave': 'pixel',
                                   'tiled': True,
                                   'blockxsize': 512,
                                   'blockysize': 512,
                                   'compress': 'DEFLATE',
                                   'predictor': self.predictor,
                                   'zlevel': 9}

                cog_translate(dts[0], out_fname,
                              default_profile,
                              indexes=[i + 1],
                              overview_resampling=resampling_method,
                              overview_level=5,
                              config=DEFAULT_GDAL_CONFIG)

        return rastercount

    def _check_tif(self, fname):
        try:
            cog_tif = gdal.Open(fname, gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except Exception as exp:
            LOG.error(f"Exception: {exp}")
            return False

        if t_stats > [0.] * 4:
            return True
        else:
            return False


def netcdf_cog_worker(wargs=None):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format using MPI
    Uses a configuration file to define the file naming schema.
    """
    netcdf_cog_fp = COGNetCDF(**list(wargs)[0])
    netcdf_cog_fp(list(wargs)[1], list(wargs)[2])


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


def get_dataset_values(product_name, time_range=None, datacube_env=None, s3_list=list()):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    query = {**dict(product=product_name), **time_range}
    dc = Datacube(app='cog-worklist query', env=datacube_env)

    field_names = get_field_names(CFG['products'][product_name])

    files = dc.index.datasets.search_returning(field_names=tuple(field_names), **query)

    for result in files:
        yield check_prefix_from_query_result(result, CFG['products'][product_name], s3_list)


def yaml_files_for_product(s3_inventory_list):
    """
    Filter the given list of s3 keys to check for '.yaml' files corresponding to a product in S3 bucket
    """
    for item in s3_inventory_list:
        if Path(item).name.endswith('.yaml'):
            return Path(item)
    return None


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


def check_prefix_from_query_result(result, product_config, s3_list):
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

    prefix = product_config['prefix'] + '/' + product_config['name_template'].format(**params)
    stack_prefix = product_config['prefix'] + '/' + product_config['stacked_name_template'].format(**params)

    # Filter the inventory list to obtain files corresponding to the product in S3 bucket
    file_list = [s3_file
                 for s3_file in s3_list
                 if s3_file.startswith(prefix) or s3_file.startswith(stack_prefix)]

    s3_key = yaml_files_for_product(file_list)

    if not s3_key:
        # Return file URI for COG conversion
        LOG.info(f"File does not exists in S3, add to file task list: {result.uri}")
        return result.uri

    # File already exists in S3
    LOG.info("File exists in S3: ")
    LOG.info(f"\tS3_path: {s3_key}")
    LOG.info(f"\tFILE URL: {result.uri}")
    return ""


# pylint: disable=invalid-name
time_range_options = click.option('--time-range', callback=validate_time_range, required=True,
                                  help="The time range:\n"
                                  " '2018-01-01 < time < 2018-12-31'  OR\n"
                                  " 'time in 2018-12-31'  OR\n"
                                  " 'time=2018-12-31'")


@click.group(help=__doc__)
def cli():
    pass


@cli.command(name='cog-convert', help="Convert a single/list of NetCDF files into COG format")
@product_options
@config_file_options
@output_dir_options
@click.option('--filelist', '-l', help='List of netcdf file names (Optional)', default=None)
@click.argument('filenames', nargs=-1, type=click.Path())
def convert_cog(product_name, config, output_dir, filelist, filenames):
    """
    Convert a single or list of NetCDF files into Cloud Optimise GeoTIFF format
    Uses a configuration file to define the file naming schema.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    global CFG
    with open(config) as config_file:
        CFG = yaml.load(config_file)

    product_config = CFG['products'][product_name]

    cog_convert = COGNetCDF(**product_config)

    # Preference is give to file list over argument list
    if filelist:
        try:
            with open(filelist) as fb:
                file_list = np.genfromtxt(fb, dtype='str')
            tasks = file_list.size
        except FileNotFoundError:
            LOG.error(f'No netCDF file found in the input path')
            raise
        else:
            if tasks == 0:
                LOG.warning(f'Task file is empty')
                sys.exit(1)
    else:
        file_list = list(filenames)

    LOG.info("Run with single process")
    for filename in file_list:
        cog_convert(filename, output_dir)


@cli.command(name='inventory-store', help="Store S3 inventory list in a pickle file")
@product_options
@config_file_options
@output_dir_options
@s3_options
@aws_profile_options
def store_s3_inventory(product_name, config, output_dir, inventory_manifest, aws_profile):
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
        if CFG['products'][product_name]['prefix'] in result.Key:
            s3_inv_list_file.append(result.Key)
    pickle_out = open(str(Path(output_dir) / product_name) + PICKLE_FILE_EXT, "wb")
    pickle.dump(s3_inv_list_file, pickle_out)
    pickle_out.close()


@cli.command(name='list-datasets', help="Generate task list for COG conversion")
@product_options
@time_range_options
@config_file_options
@output_dir_options
@dc_env_options
@s3_options
@aws_profile_options
@s3_inventory_list_options
def generate_work_list(product_name, time_range, config, output_dir, datacube_env,
                       inventory_manifest, aws_profile, s3_list):
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

    if not s3_list:
        for result in list_inventory(inventory_manifest, s3=make_s3_client(profile=aws_profile),
                                     aws_profile=aws_profile):
            # Get the list for the product we are interested
            if CFG['products'][product_name]['prefix'] in result.Key:
                s3_list.append(result.Key)
    else:
        pickle_in = open(str(Path(output_dir) / product_name) + PICKLE_FILE_EXT, "rb")
        s3_list = pickle.load(pickle_in)

    dc_file_list = set([uri
                        for uri in get_dataset_values(product_name,
                                                      parse_expressions(time_range),
                                                      datacube_env,
                                                      s3_list)
                        if uri])
    out_file = str(Path(output_dir) / product_name) + TASK_FILE_EXT

    with open(out_file, 'w') as fp:
        for nc_filepath in sorted(dc_file_list):
            fp.write(nc_filepath.split('//')[1] + '\n')


@cli.command(name='mpi-cog-convert')
@product_options
@config_file_options
@output_dir_options
@click.argument('filelist', nargs=1, required=True)
def mpi_cog_convert(product_name, config, output_dir, filelist):
    """
    \b
    Parallelise COG convert using MPI
    Example: mpirun --oversubscribe -n 5 python3 streamer.py mpi-cog-convert -c aws_products_config.yaml
             --output-dir /tmp/wofls_cog/ -p wofs_albers /tmp/wofs_albers_file_list
                where,
                    -n is the total number of processes required for COG conversion
    \f
    Convert netcdf files to COG format using parallelisation by MPI tool.
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
        with open(filelist) as fb:
            file_list = np.genfromtxt(fb, dtype='str')
        tasks = file_list.size
    except FileNotFoundError:
        LOG.error(f'MPI Worker ({MPI_JOB_RANK}): No netCDF file/s found in the input path')
        raise
    else:
        if tasks == 0:
            LOG.warning(f'MPI Worker ({MPI_JOB_RANK}): Task file is empty')
            sys.exit(1)

    product_config = CFG['products'][product_name]
    num_workers = MPI_JOB_SIZE-1 if MPI_JOB_SIZE > 1 else _raise_value_err(
        f"MPI Worker ({MPI_JOB_RANK}): Number of required processes has to be greater than 1")

    # Ensure all errors/exceptions are handled before this, else master-worker processes
    # will enter a dead-lock situation
    if MPI_JOB_RANK == 0:
        name = MPI.Get_processor_name()
        task_index = 0
        closed_workers = 0
        job_args = []
        LOG.debug(f"MPI Master ({MPI_JOB_RANK}) on {name} node, starting with {num_workers} workers")

        # Append the jobs_args list for each filename to be scheduled among all the available workers
        if tasks == 1:
            job_args = [(product_config, str(file_list), output_dir)]
        elif tasks > 1:
            for filename in file_list:
                job_args.extend([(product_config, str(filename), output_dir)])

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
                    MPI_COMM.send(None, dest=source, tag=TagStatus.EXIT)
            elif tag == TagStatus.DONE:
                LOG.debug(f"MPI Worker ({source}) on {name} completed the assigned task")
            elif tag == TagStatus.EXIT:
                LOG.debug(f"MPI Worker ({source}) exited")
                closed_workers += 1

        LOG.debug("Batch processing completed")
    else:
        proc_name = MPI.Get_processor_name()

        while True:
            MPI_COMM.send(None, dest=0, tag=TagStatus.READY)
            task = MPI_COMM.recv(source=0, tag=MPI.ANY_TAG, status=MPI_JOB_STATUS)
            tag = MPI_JOB_STATUS.Get_tag()

            if tag == TagStatus.START:
                LOG.debug(f"MPI Worker ({MPI_JOB_RANK}) on {proc_name} started COG conversion")
                netcdf_cog_worker(wargs=task)
                MPI_COMM.send(None, dest=0, tag=TagStatus.DONE)
            elif tag == TagStatus.EXIT:
                break

        LOG.debug(f"MPI Worker ({MPI_JOB_RANK}) did not receive any task, hence sending exit status to the master")
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
    gtiff_file_list = sorted(Path(path).rglob("*.tif"))
    count = 0
    for file_path in gtiff_file_list:
        count += 1
        command = f"python3 {VALIDATE_GEOTIFF_PATH} {file_path}"
        output = subprocess.getoutput(command)
        valid_output = (output.split('/'))[-1]

        if 'is a valid cloud' in valid_output:
            LOG.info(f"{count}: {valid_output}")
        else:
            LOG.error(f"{count}: {valid_output}")

            # List erroneous *.tif file path
            LOG.error(f"\tErroneous GeoTIFF filepath: {file_path}")
            REMOVE_ROOT_GEOTIFF_DIR.append(file_path.parent)
            for files in file_path.parent.rglob('*.*'):
                # List all files associated with erroneous *.tif file and remove them
                INVALID_GEOTIFF_FILES.append(files)

    for rm_files in set(INVALID_GEOTIFF_FILES):
        LOG.error(f"\tDelete {rm_files} file")
        rm_files.unlink()

    for rm_dir in set(REMOVE_ROOT_GEOTIFF_DIR):
        LOG.error(f"\tDelete {rm_dir} directory")
        rm_dir.rmdir()


@cli.command(name='qsub-cog-convert', help="Kick off four stage COG Conversion PBS job")
@product_options
@time_range_options
@config_file_options
@output_dir_options
@queue_options
@project_options
@node_options
@walltime_options
@mail_options
@mail_id
@s3_options
@aws_profile_options
@dc_env_options
def qsub_cog_convert(product_name, time_range, config, output_dir, queue, project, nodes, walltime,
                     email_options, email_id, inventory_manifest, aws_profile, datacube_env):
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

    prep = 'qsub -q copyq -N store_s3_list_%(product)s -P %(project)s ' \
           '-l wd,walltime=5:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           'module use /g/data/v10/public/modules/modulefiles/; ' \
           'module load dea; ' \
           'python3 %(streamer_file)s inventory-store -c %(yaml_file)s ' \
           '--inventory-manifest %(s3_inventory)s --aws-profile %(aws_profile)s --product-name %(product)s ' \
           '--output-dir %(output_dir)s"'
    cmd = prep % dict(product=product_name, project=project, streamer_file=STREAMER_FILE_PATH,
                      yaml_file=config, s3_inventory=inventory_manifest, aws_profile=aws_profile,
                      output_dir=output_dir)

    s3_inv_job = run_command(cmd)

    prep = 'qsub -q %(queue)s -N generate_work_list_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(s3_inv_job)s: -l wd,walltime=10:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc;' \
           '%(generate_script)s --dea-module %(dea_module)s --streamer-file %(streamer_file)s ' \
           '--config-file %(yaml_file)s --product-name %(product)s --output-dir %(output_dir)s ' \
           '--datacube-env %(dc_env)s --s3-list %(pickle_file)s --time-range \'%(time_range)s\'"'
    cmd = prep % dict(queue=queue, product=product_name, project=project, s3_inv_job=s3_inv_job.split('.')[0],
                      generate_script=GENERATE_FILE_PATH, dea_module=digitalearthau.MODULE_NAME,
                      streamer_file=STREAMER_FILE_PATH, yaml_file=config, output_dir=output_dir,
                      dc_env=datacube_env, pickle_file=str(Path(output_dir) / product_name) + PICKLE_FILE_EXT,
                      time_range=time_range)

    file_list_job = run_command(cmd)

    qsub = 'qsub -q %(queue)s -N mpi_cog_convert_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(file_list_job)s: -m %(email_options)s -M %(email_id)s ' \
           '-l ncpus=%(ncpus)d,mem=%(mem)dgb,jobfs=10GB,walltime=%(walltime)d:00:00,wd ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           'module use /g/data/v10/public/modules/modulefiles/; ' \
           'module load dea; ' \
           'mpirun --oversubscribe -n %(ncpus)d ' \
           'python3 %(streamer_file)s mpi-cog-convert -c %(yaml_file)s ' \
           '--output-dir %(output_dir)s --product-name %(product)s %(file_list)s"'
    cmd = qsub % dict(queue=queue,
                      project=project,
                      file_list_job=file_list_job.split('.')[0],
                      email_options=email_options,
                      email_id=email_id,
                      ncpus=nodes * 16,
                      mem=nodes * 31,
                      walltime=walltime,
                      streamer_file=STREAMER_FILE_PATH,
                      yaml_file=config,
                      output_dir=str(Path(output_dir) / product_name),
                      product=product_name,
                      file_list=task_file)
    cog_conversion_job = run_command(cmd)

    prep = 'qsub -q copyq -N s3_sync_%(product)s -P %(project)s ' \
           '-W depend=afterok:%(cog_conversion_job)s: ' \
           '-l wd,walltime=5:00:00,mem=31GB,jobfs=5GB -W umask=33 ' \
           '-- /bin/bash -l -c "source $HOME/.bashrc; ' \
           '%(aws_sync_script)s --dea-module %(dea_module)s ' \
           '--inventory-manifest %(s3_inventory)s --aws-profile %(aws_profile)s ' \
           '--output-dir %(output_dir)s --streamer-file %(streamer_file)s"'
    cmd = prep % dict(product=product_name, project=project,
                      cog_conversion_job=cog_conversion_job.split('.')[0],
                      aws_sync_script=AWS_SYNC_PATH, dea_module=digitalearthau.MODULE_NAME,
                      s3_inventory=inventory_manifest, streamer_file=STREAMER_FILE_PATH, aws_profile=aws_profile,
                      output_dir=str(Path(output_dir) / product_name))
    run_command(cmd)


if __name__ == '__main__':
    cli()

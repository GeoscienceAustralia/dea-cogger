"""
Bulk convert ODC Datasets to Cloud Optimised GeoTIFF and sync them to AWS S3

To use on the NCI, load the ``dea`` module by running::

    $ module use /g/data/v10/public/modules/modulefiles/
    $ module load dea
"""
import csv
import os
import shutil
import socket
import subprocess
import sys
from functools import partial
from os.path import splitext
from pathlib import Path

import click
import structlog
import yaml
from datacube.ui.expression import parse_expressions
from tqdm import tqdm

from dea_cogger.aws_inventory import list_inventory
from dea_cogger.cogeo import NetCDFCOGConverter
from dea_cogger.utils import get_dataset_values, validate_time_range, _convert_cog, expected_bands

LOG = structlog.get_logger()

PACKAGE_DIR = Path(__file__).absolute().parent
CONFIG_FILE_PATH = PACKAGE_DIR / 'aws_products_config.yaml'
VALIDATE_GEOTIFF_CMD = PACKAGE_DIR / 'validate_cloud_optimized_geotiff.py'
S3_LIST_EXT = '_s3_inv_list.txt'
TASK_FILE_EXT = '_file_list.txt'

# pylint: disable=invalid-name
output_dir_option = click.option('--output-dir', '-o', required=True,
                                 type=click.Path(exists=True, writable=True),
                                 help='Output destination directory')

product_option = click.option('--product-name', '-p', required=True,
                              help="Product name as defined in product configuration file")

s3_output_dir_option = click.option('--s3-output-url', default=None, required=True,
                                    metavar='S3_URL',
                                    help="S3 URL for uploading the converted files")

s3_inv_option = click.option('--inventory-manifest', '-i',
                             default='s3://dea-public-data-inventory/dea-public-data/dea-public-data-csv-inventory/',
                             show_default=True,
                             metavar='S3_URL',
                             help="The manifest of AWS S3 bucket inventory URL")

config_file_option = click.option('--config', '-c', default=CONFIG_FILE_PATH,
                                  show_default=True,
                                  type=click.Path(exists=True),
                                  help='Product configuration file')


@click.group(help=__doc__,
             context_settings=dict(max_content_width=200))  # Will still shrink to screen width
def cli():
    # See https://github.com/tqdm/tqdm/issues/313
    hostname = socket.gethostname()
    proc_id = os.getpid()

    def add_proc_info(_, logger, event_dict):
        event_dict["hostname"] = hostname
        event_dict["pid"] = proc_id
        return event_dict

    try:
        from mpi4py import MPI
        mpi_rank = MPI.COMM_WORLD.rank  # Rank of this process
        mpi_size = MPI.COMM_WORLD.size  # Rank of this process

        def add_mpi_rank(_, logger, event_dict):
            if mpi_size > 1:
                event_dict['mpi_rank'] = mpi_rank
            return event_dict
    except ImportError:
        def add_mpi_rank(_, __, event_dict):
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


@cli.command(help="Convert files into COG format")
@product_option
@output_dir_option
@config_file_option
@click.option('--filelist', '-l', help='List of input file names', default=None)
def convert(product_name, output_dir, config, filelist):
    """
    Convert NetCDF files into Cloud Optimised GeoTIFF format

    Use a configuration file to define the file naming schema.

    Before using this command, execute the following:

      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
    """
    with open(config) as config_file:
        _CFG = yaml.safe_load(config_file)

    product_config = _CFG['products'][product_name]

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


@cli.command(name='save-s3-inventory', help="Save S3 inventory list in a text file")
@product_option
@output_dir_option
@config_file_option
@s3_inv_option
def save_s3_inventory(product_name, output_dir, config, inventory_manifest):
    """
    Save a list of S3 objects stored for a product

    Requires S3 Inventory to be set up for the target bucket.

    Save those file into a text file for further processing.
    Uses a configuration file to define the file naming schema.

    """
    with open(config) as config_file:
        config = yaml.safe_load(config_file)

    prefix = config['products'][product_name]['prefix']
    with open(Path(output_dir) / (product_name + S3_LIST_EXT), 'wt') as outfile:
        for result in list_inventory(inventory_manifest):
            # Get the list for the product we are interested
            if result.Key.startswith(prefix):
                outfile.write(result.Key + '\n')


@cli.command(help='Download an entire S3 Inventory and save in an efficient DAWG file')
@s3_inv_option
@click.argument('output-file')
def save_dawg(output_file, inventory_manifest):
    import dawg
    s3_objs = (rec.Key for rec in list_inventory(inventory_manifest))
    dawg = dawg.DAWG(s3_objs)

    dawg.save(output_file)


@cli.command(name='generate-work-list', help="""Generate task list for COG conversion""")
@product_option
@output_dir_option
@click.option('--s3-list', default=None,
              type=click.Path(exists=True),
              help='Text file containing the list of existing s3 keys')
@click.option('--time-range', callback=validate_time_range,
              default="",
              help="The time range, eg:\n"
                   " time in 2020  OR\n"
                   " 'time in 2018-12-31'  OR\n"
                   " 'time in [2018-12-01, 2018-12-31]'")
@config_file_option
def generate_work_list(product_name, output_dir, s3_list, time_range, config):
    """
    Compares datacube file uri's against S3 bucket (file names within text file) and writes the list of datasets
    for cog conversion into the task file
    Uses a configuration file to define the file naming schema.

    """
    with open(config) as config_file:
        config = yaml.safe_load(config_file)

    if s3_list is None:
        # Use text file generated by save_s3_inventory function
        s3_list = Path(output_dir) / (product_name + S3_LIST_EXT)

    with open(s3_list, "r") as f:
        existing_s3_keys = set(l.strip() for l in f.readlines())

    # Mapping from Expected Output YAML Location -> Input NetCDF File
    dc_workgen_list = dict()

    eb = expected_bands(product_name)

    for source_uri, new_basename in get_dataset_values(product_name,
                                                       config['products'][product_name],
                                                       parse_expressions(time_range)):
        output_yaml = new_basename + '.yaml'
        expected_outputs = [f'{new_basename}_{band}.tif' for band in eb] + [output_yaml]
        if not all(output in existing_s3_keys for output in expected_outputs):
            dc_workgen_list[new_basename] = source_uri.split('file://')[1]

    out_file = Path(output_dir) / (product_name + TASK_FILE_EXT)

    with open(out_file, 'w', newline='') as fp:
        csv_writer = csv.writer(fp, quoting=csv.QUOTE_MINIMAL)
        LOG.info(f'Found {len(dc_workgen_list)} datasets needing conversion, writing to {out_file}')
        for s3_basename, input_file in dc_workgen_list.items():
            LOG.debug(f"File does not exists in S3, add to processing list: {input_file}")
            # Write Input_file, Output Basename
            csv_writer.writerow((input_file, splitext(s3_basename)[0]))

    if not dc_workgen_list:
        LOG.info(f"No tasks found, everything is up to date.")


@cli.command(name='mpi-convert', help='Bulk COG conversion using MPI')
@product_option
@output_dir_option
@config_file_option
@click.argument('filelist', nargs=1, required=True)
def mpi_convert(product_name, output_dir, config, filelist):
    """
    Iterate over the file list and assign MPI worker for processing.
    Split the input file by the number of workers, each MPI worker completes every nth task.
    Also, detect and fail early if not using full resources in an MPI job.

    Before using this command, execute the following:
      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load dea
      $ module load openmpi
    """
    job_rank, job_size = _mpi_init()

    with open(config) as cfg_file:
        config = yaml.safe_load(cfg_file)

    try:
        fl = open(filelist)
        tasks = csv.reader(fl)
    except FileNotFoundError:
        LOG.error('Task file not found.', filepath=filelist)
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


def _nth_by_mpi(iterator):
    """
    Use to split an iterator based on MPI pool size and rank of this process
    """
    from mpi4py import MPI
    job_size = MPI.COMM_WORLD.size  # Total number of processes
    job_rank = MPI.COMM_WORLD.rank  # Rank of this process
    for i, element in enumerate(iterator):
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
    :param rm_broken: Remove broken (directories with corrupted cog files or without yaml file) directories
    :return: None
    """
    job_rank, job_size = _mpi_init()
    broken_files = set()

    path = Path(path)

    if path.is_dir():
        # Non-lazy recursive search for geotiffs
        gtiff_file_list = Path(path).rglob("*.[tT][iI][fF]")
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
        # TODO: Call directly instead of instanciating more python interpreters!
        command = f"python3 {VALIDATE_GEOTIFF_CMD} {geotiff_file}"
        exitcode, output = subprocess.getstatusoutput(command)
        validator_output = output.split('/')[-1]

        # If no metadata file does not exists after cog conversion then add the tiff file to the broken files set
        if not list(geotiff_file.parent.rglob('*.yaml')):
            LOG.error("No YAML file created for GeoTIFF file", error=validator_output, filename=geotiff_file)
            broken_files.add(geotiff_file)

        if exitcode == 0:
            LOG.debug(validator_output)
        else:
            # Log and remember broken COG
            LOG.error("Invalid GeoTIFF file", error=validator_output, filename=geotiff_file)
            broken_files.add(geotiff_file)

    if rm_broken:
        # Delete directories containing broken files
        # Prevent deleting directories out from under another worker that's checking files within.
        from mpi4py import MPI
        comm = MPI.COMM_WORLD

        gathered = comm.gather(broken_files, root=0)
        if job_rank == 0:
            broken_files = set().union(*gathered)
        else:
            assert gathered is None
            return

        broken_directories = set(file.parent for file in broken_files)

        for directory in broken_directories:
            LOG.info('Deleting directory', dir=directory)
            shutil.rmtree(directory)


if __name__ == '__main__':
    cli()

#!/usr/bin/env python
"""
This program is designed to stream COG-conversion and AWS uploads to be done same time
in a streaming fashion that utilises smaller in-between storage footprint. The program is designed
to so that it could run COG-conversion and AWS uploads in separate processes, i.e. multiple runs
of the same script concurrently. A database-based queue is used for messaging between COG-conversion
and AWS upload processes. The database queue is currently implemented with sqlite.

The program utilizes an aligned file system queue with messaging queue of processed files to hook up
COG-conversion and AWS upload processes.

A particular batch run is identified by the signature (product, year, month) combination where
year and/or month may not be present. The job control tracks a particular batch job based on this signature
and the relevant log files are kept in the job control directory specified by the '--job' option (short, '-j').
There are two types of job control files kept, one with a template 'streamer_job_control_<signature>.log'
and one with a template 'items_all_<signature>.log', where the first tracks the batch run incrementally maintaining
the NetCDF files that are processed while the second keeps the list of all the files that are part of a specific
batch job. However, the script need to be told to keep 'items_all_<signature>.log' via the '--reuse_full_list'
option so that it does not need to recompute in the next incremental run of the same batch job
if it is not yet complete. The list of all files to be processed can be reliably computed by using
the option '--use_datacube' which computes the file list based on what is indexed in the datacube.

The '--limit' option, which specifies the number of files to be processed, can be used to incrementally
run a specific batch job. Alternatively, multiple overlapping runs of the program can be done with
'--file_range' option which specifies the range of files (indexes into 'items_all_<signature>.log').
For example, if items_all_<signature>.log' has 200 files, you can specify --file_range 0 99 in one of
the runs and --file_range 100 199 in another run.

The program uses a config, that in particular specify product descriptions such as whether it is timed/flat,
source and destination filename templates, source file system organization such as tiled/flat, aws directory,
and bucket. The destination template must only specify the prefix of the file excluding the band name details and
extension. An example such config spec for a product is as follows:

    ls5_fc_albers:
        time_type: timed
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        src_dir_type: tiled
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        bucket: s3://dea-public-data-dev



When specifying command line options for cog_only and upload_only overlapping runs, it is important
that consistent command line options are specified so that both processes refer to the same
'items_all_<signature>.log' and 'streamer_job_control_<signature>.log'. Without this, the two processes
may not be in sync.


"""
import logging
import os
import re
import subprocess
from subprocess import call, check_output
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, wait, as_completed
from datetime import datetime
from os.path import join as pjoin, basename
from subprocess import run

import click
import gdal
import xarray
import yaml
from datacube import Datacube
from datacube.model import Range
from netCDF4 import Dataset
from pandas import to_datetime
from pq import PQ
from psycopg2 import connect, ProgrammingError
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

LOG = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 4
WORKERS_POOL = 4

DEFAULT_CONFIG = """
queue_db:
    db_name: aj9439_db
    host: agdcdev-db.nci.org.au
    port: 6432  # Does not work with PgBouncer, must use 5432
products: 
    wofs_albers: 
        time_type: timed
        src_template: LS_WATER_3577_{x}_{y}_{time}_v{}.nc 
        dest_template: LS_WATER_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf
        src_dir_type: tiled
        aws_dir: WOfS/WOFLs/v2.1.0/combined
        bucket:  s3://dea-public-data-dev
    wofs_filtered_summary:
        time_type: flat
        src_template: wofs_filtered_summary_{x}_{y}.nc
        dest_template: wofs_filtered_summary_{x}_{y}
        src_dir: /g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf
        src_dir_type: flat
        aws_dir: WOfS/filtered_summary/v2.1.0/combined
        bucket: s3://dea-public-data-dev
    ls5_fc_albers:
        time_type: timed
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        src_dir_type: tiled
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        bucket: s3://dea-public-data-dev
    ls7_fc_albers:
        time_type: timed
        src_template: LS7_ETM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS7_ETM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS7_ETM_FC
        src_dir_type: tiled
        aws_dir: fractional-cover/fc/v2.2.0/ls7
        bucket: s3://dea-public-data-dev        
    ls8_fc_albers:
        time_type: timed
        src_template: LS8_OLI_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS8_OLI_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS8_OLI_FC
        src_dir_type: tiled
        aws_dir: fractional-cover/fc/v2.2.0/ls8
        bucket: s3://dea-public-data-dev
"""


def run_command(command, work_dir=None):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        run(command, stderr=subprocess.STDOUT, cwd=work_dir, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def upload_to_s3(product_config, input_file, src_dir, dest_url, job_file):
    """
    Uploads the .yaml and .tif files that correspond to the given NetCDF 'file' into the AWS
    destination bucket indicated by 'dest'.

    Once complete add the file name 'file' to the 'job_file'.

    Each NetCDF file is assumed to be a stacked NetCDF file where 'unstacked' NetCDF file is viewed as
    a stacked file with just one dataset. The directory structure in AWS is determined by the
    'JobControl.aws_dir()' based on 'prefixes' extracted from the NetCDF file.

    Each dataset within the NetCDF file is assumed to be in separate directory with the name indicated by its corresponding
    prefix. The 'prefix' would have structure as in 'LS_WATER_3577_9_-39_20180506102018000000'.
    """

    prefix_names = product_config.get_unstacked_names(input_file)
    success = True
    for prefix in prefix_names:
        src = os.path.join(src_dir, prefix)
        item_dir = product_config.aws_dir(prefix)
        dest_path = f'{dest_url}/{item_dir}'

        aws_copy = [
            'aws',
            's3',
            'sync',
            src,
            dest_path
        ]
        try:
            run_command(aws_copy)
        except Exception as e:
            success = False
            logging.error("AWS upload error %s", prefix)
            logging.exception("Exception", e)

        # Remove the dir from the queue directory
        try:
            run('rm -fR -- ' + src, stderr=subprocess.STDOUT, check=True, shell=True)
        except Exception as e:
            success = False
            logging.error("Failure in queue: removing dataset %s", prefix)
            logging.exception("Exception", e)

    # job control logs
    if success:
        with open(job_file, 'a') as f:
            f.write(input_file + '\n')


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
    """

    @staticmethod
    def netcdf_to_cog(input_file, dest_dir, product_config):
        """
        Convert the datasets in the NetCDF file 'file' into 'dest_dir'

        Each dataset is put in a separate directory.

        The directory names will look like 'LS_WATER_3577_9_-39_20180506102018'
        """

        prefix_names = product_config.get_unstacked_names(input_file)

        dataset_array = xarray.open_dataset(input_file)
        dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()

        for index, prefix in enumerate(prefix_names):
            prefix = prefix_names[index]
            dest = os.path.join(dest_dir, prefix)
            os.makedirs(dest, exist_ok=True)

            # Read the Dataset Metadata from the 'dataset' variable in the NetCDF file, and save as YAML
            dataset_item = dataset_array.dataset.item(index)
            COGNetCDF._dataset_to_yaml(prefix, dataset_item, dest)

            # Extract each band from the NetCDF and write to individual GeoTIFF files
            COGNetCDF._dataset_to_cog(prefix, subdatasets, index + 1, dest)

            # Clean up XML files from GDAL
            # GDAL creates extra XML files which we don't want
            run('rm -fR -- ' + dest + '/*.xml', stderr=subprocess.STDOUT, check=True, shell=True)

        return input_file

    @staticmethod
    def _dataset_to_yaml(prefix, nc_dataset: xarray.DataArray, dest_dir):
        """
        Write the datasets to separate yaml files
        """
        yaml_fname = os.path.join(dest_dir, prefix + '.yaml')
        dataset_object = nc_dataset.decode('utf-8')
        dataset = yaml.load(dataset_object, Loader=Loader)

        # Update band urls
        for key, value in dataset['image']['bands'].items():
            value['layer'] = '1'
            value['path'] = prefix + '_' + key + '.tif'

        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}
        with open(yaml_fname, 'w') as fp:
            yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
            logging.info("Writing dataset Yaml to %s", basename(yaml_fname))

    @staticmethod
    def _dataset_to_cog(prefix, subdatasets, band_num, dest_dir):
        """
        Write the datasets to separate cog files
        """

        os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
        os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'

        with tempfile.TemporaryDirectory() as tmpdir:
            for dts in subdatasets[:-1]:
                band_name = dts[0].split(':')[-1]
                out_fname = prefix + '_' + band_name + '.tif'
                try:

                    # copy to a tempfolder
                    temp_fname = pjoin(tmpdir, basename(out_fname))
                    to_cogtif = [
                        'gdal_translate',
                        '-of',
                        'GTIFF',
                        '-b',
                        str(band_num),
                        dts[0],
                        temp_fname]
                    run_command(to_cogtif, tmpdir)

                    # Add Overviews
                    # gdaladdo - Builds or rebuilds overview images.
                    # 2, 4, 8,16,32 are levels which is a list of integral overview levels to build.
                    add_ovr = [
                        'gdaladdo',
                        '-r',
                        'nearest',
                        '--config',
                        'GDAL_TIFF_OVR_BLOCKSIZE',
                        '512',
                        temp_fname,
                        '2',
                        '4',
                        '8',
                        '16',
                        '32']
                    run_command(add_ovr, tmpdir)

                    # Convert to COG
                    cogtif = [
                        'gdal_translate',
                        '-co',
                        'TILED=YES',
                        '-co',
                        'COPY_SRC_OVERVIEWS=YES',
                        '-co',
                        'COMPRESS=DEFLATE',
                        '-co',
                        'ZLEVEL=9',
                        '--config',
                        'GDAL_TIFF_OVR_BLOCKSIZE',
                        '512',
                        '-co',
                        'BLOCKXSIZE=512',
                        '-co',
                        'BLOCKYSIZE=512',
                        '-co',
                        'PREDICTOR=2',
                        '-co',
                        'PROFILE=GeoTIFF',
                        temp_fname,
                        out_fname]
                    run_command(cogtif, dest_dir)
                except Exception as e:
                    logging.error("Failure during COG conversion: %s", out_fname)
                    logging.exception("Exception", e)


class COGProductConfiguration:
    """
    Utilities and some hardcoded stuff for tracking and coding job info.

    :param dict cfg: Configuration for the product we're processing
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def aws_dir(self, item):
        """
        Given a prefix like 'LS_WATER_3577_9_-39_20180506102018000000' what is the AWS directory structure?
        """

        if self.cfg['time_type'] == 'flat':
            x, y = self._extract_x_y(item, self.cfg['dest_template'])
            return os.path.join('x_' + x, 'y_' + y)
        elif self.cfg['time_type'] == 'timed':
            x, y, time_stamp = self._extract_x_y_time(item, self.cfg['dest_template'])
            year = time_stamp[0:4]
            month = time_stamp[4:6]
            day = time_stamp[6:8]
            return f'x_{x}/y_{y}/{year}/{month}/{day}'
        else:
            raise RuntimeError("Incorrect product time_type")

    @staticmethod
    def _extract_x_y(item, template):
        tem = template.replace("{x}", "(?P<x>-?[0-9]*)")
        tem = tem.replace("{y}", "(?P<y>-?[0-9]*)")
        tem = tem.replace("{time}", "[0-9]*")
        tem = tem.replace("{}", "[0-9]*")
        values = re.compile(tem).match(basename(item))
        if not values:
            raise RuntimeError("There is no tile index values in item")
        values = values.groupdict()
        if 'x' in values.keys() and 'y' in values.keys():
            return values['x'], values['y']
        else:
            raise RuntimeError("There is no tile index values in item")

    @staticmethod
    def _extract_x_y_time(item, template):
        tem = template.replace("{x}", "(?P<x>-?[0-9]*)")
        tem = tem.replace("{y}", "(?P<y>-?[0-9]*)")
        tem = tem.replace("{time}", "(?P<time>-?[0-9]*)")
        tem = tem.replace("{}", "[0-9]*")
        values = re.compile(tem).match(basename(item))
        if not values:
            raise RuntimeError("There is no tile index values in prefix")
        values = values.groupdict()
        if 'x' in values.keys() and 'y' in values.keys() and 'time' in values.keys():
            return values['x'], values['y'], values['time']
        else:
            raise RuntimeError("There is no tile index values in prefix")

    def get_unstacked_names(self, netcdf_file, year=None, month=None):
        """
        Return the dataset prefix names corresponding to each dataset within the given NetCDF file.
        """

        names = []
        x, y = self._extract_x_y(netcdf_file, self.cfg['src_template'])
        if self.cfg['time_type'] == 'flat':
            # only extract x and y
            names.append(self.cfg['dest_template'].format(x=x, y=y))
        else:
            # if time_type is not flat we assume it is timed
            dts = Dataset(netcdf_file)
            dts_times = dts.variables['time']
            for index, dt in enumerate(dts_times):
                dt_ = datetime.fromtimestamp(dt)
                # With nanosecond -use '%Y%m%d%H%M%S%f'
                time_stamp = to_datetime(dt_).strftime('%Y%m%d%H%M%S')
                if year:
                    if month:
                        if dt_.year == year and dt_.month == month:
                            names.append(self.cfg['dest_template'].format(x=x, y=y, time=time_stamp))
                    elif dt_.year == year:
                        names.append(self.cfg['dest_template'].format(x=x, y=y, time=time_stamp))
                else:
                    names.append(self.cfg['dest_template'].format(x=x, y=y, time=time_stamp))
        return names


def get_indexed_files(product, year=None, month=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    query = {'product': product}
    if year and month:
        query['time'] = Range(datetime(year=year, month=month, day=1), datetime(year=year, month=month + 1, day=1))
    elif year:
        query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    dc = Datacube(app='streamer', env=datacube_env)
    files = dc.index.datasets.search_returning(field_names=('uri',), **query)

    # TODO: For now, turn the URL into a file name by removing the schema and #part. Should be made more robust
    def filename_from_uri(uri):
        return uri[0].split(':')[1].split('#')[0]

    return set(filename_from_uri(uri) for uri in files)


def get_queue(queue_name):
    connection = connect(dbname='aj9439_db', host='agdcdev-db.nci.org.au', port='6432')
    cur = connection.cursor()
    cur.execute("DEALLOCATE PREPARE ALL")

    pq = PQ(conn=connection)
    try:
        pq.create()
    except ProgrammingError as exc:
        # We ignore a duplicate table error.
        if exc.pgcode != '42P07':
            raise

    return pq[queue_name]


@click.group()
def cli():
    pass


@cli.command()
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
def generate_work_list(product_name, year, month):
    items_all = get_indexed_files(product_name, year, month)

    for item in sorted(items_all):
        print(item)


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--num-procs', type=int, default=1, help='Number of processes to parallelise across')
@click.argument('filenames', nargs=-1, type=click.Path())
def convert_cog(config, output_dir, product, num_procs, filenames):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format

    Uses a configuration file to define the file naming schema.

    Can optionally record completed conversions into a PostgreSQL queue.
    """
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    product_config = COGProductConfiguration(cfg['products'][product])

    thequeue = get_queue(product)

    with ProcessPoolExecutor(max_workers=num_procs) as executor:

        futures = (
            executor.submit(COGNetCDF.netcdf_to_cog, filename, dest_dir=output_dir, product_config=product_config)
            for filename in filenames)

        for future in as_completed(futures):
            # Submit to completed Queue
            result = future.result()
            print(future.result())
            thequeue.put(result)


@cli.command()
@click.option('--config', '-c', help="Config file")
@click.option('--product', '-p', help='Product name', required=True)
def upload(config, product):
    """
    Connect to the PQ queue of completed COGs and upload them to S3

    Will wait for new items for up to 3 minutes...

    :param connection_string:
    :param queue_name:
    :return:
    """
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    ready_dir = '/g/data/u46/users/aj9439/aws/queue/wofs_albers'

    while True:
        datasets_ready = check_output(['ls', ready_dir]).decode('utf-8').splitlines()
        for dataset in datasets_ready:
            src_path = f'{ready_dir}/{dataset}'
            dest_file = f'{src_path}/upload-destination.txt'
            if os.path.exists(dest_file):
                with open(dest_file) as f:
                    dest_path = f.read().splitlines()[0]
                call(['rm', dest_file])
                aws_copy = [
                    'aws',
                    's3',
                    'sync',
                    src_path,
                    dest_path
                ]
                try:
                    print(dest_path)
                    # run_command(aws_copy)
                except Exception as e:
                    logging.error("AWS upload error %s", dest_path)
                    logging.exception("Exception", e)
                else:
                    try:
                        run('rm -fR -- ' + src_path, stderr=subprocess.STDOUT, check=True, shell=True)
                    except Exception as e:
                        logging.error("Failure in queue: removing dataset %s", src_path)
                        logging.exception("Exception", e)

#    for completed_dataset_directory, destination_url in queue:

#        upload_to_s3(completed_dataset_directory, destination_url)


if __name__ == '__main__':
    cli()

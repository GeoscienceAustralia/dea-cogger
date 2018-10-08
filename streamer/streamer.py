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
import tempfile
import threading
from concurrent.futures import ProcessPoolExecutor, wait, as_completed
from datetime import datetime
from functools import reduce
from multiprocessing import Pool
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
from psycopg2 import connect, ProgrammingError, OperationalError
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

LOG = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 4
WORKERS_POOL = 4

DEFAULT_CONFIG = """
queue_db:
    db_name: aj9439_db
    host: agdcdev-db.nci.org.au
    port: 5432  # Does not work with PgBouncer, must use 5432
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


def upload_to_s3(job_control, input_file, src_dir, dest_url, job_file):
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

    prefix_names = job_control.get_unstacked_names(input_file)
    success = True
    for prefix in prefix_names:
        src = os.path.join(src_dir, prefix)
        item_dir = job_control.aws_dir(prefix)
        dest_path = f'{dest_url}/{item_dir}'

        # GDAL creates extra XML files which we don't want
        try:
            run('rm -fR -- ' + src + '/*.xml', stderr=subprocess.STDOUT, check=True, shell=True)
        except Exception as e:
            success = False
            logging.error("Failure in queue: removing datasets *.xml")
            logging.exception("Exception", e)

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
    """ Bunch of utilities for COG conversion of NetCDF files"""

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

        env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
               'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']

        with tempfile.TemporaryDirectory() as tmpdir:
            for dts in subdatasets[:-1]:
                band_name = dts[0].split(':')[-1]
                out_fname = prefix + '_' + band_name + '.tif'
                try:

                    # copy to a tempfolder
                    temp_fname = pjoin(tmpdir, basename(out_fname))
                    to_cogtif = env + [
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
                    add_ovr = env + [
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
                    cogtif = env + [
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

    @staticmethod
    def datasets_to_cog(job_control, input_file, dest_dir):
        """
        Convert the datasets in the NetCDF file 'file' into 'dest_dir' where each dataset is in
        a separate directory with the name indicated by the dataset prefix. The prefix would look
        like 'LS_WATER_3577_9_-39_20180506102018000000'
        """

        file_names = job_control.get_unstacked_names(input_file)

        dataset_array = xarray.open_dataset(input_file)
        with gdal.Open(input_file, gdal.GA_ReadOnly) as dataset:
            subdatasets = dataset.GetSubDatasets()

        for index, prefix in enumerate(file_names):
            prefix = file_names[index]
            dataset_item = dataset_array.dataset.item(index)
            dest = os.path.join(dest_dir, prefix)

            COGNetCDF._dataset_to_yaml(prefix, dataset_item, dest)
            COGNetCDF._dataset_to_cog(prefix, subdatasets, index + 1, dest)
        return input_file


class TileFiles:
    """ A utility class used by multiprocess routines to compute the NetCDF file list for a product"""

    def __init__(self, year=None, month=None):
        self.year = year
        self.month = month

    @staticmethod
    def check(file, year, month):
        name, ext = os.path.splitext(basename(file))
        if ext == '.nc':
            time_stamp = name.split('_')[-2]
            if year:
                if int(time_stamp[0:4]) == year:
                    if month:
                        if len(time_stamp) >= 6 and int(time_stamp[4:6]) == month:
                            return True
                    else:
                        return True
            else:
                return True
        return False

    def process_tile_files(self, tile_dir):
        names = []
        for top, dirs, files in os.walk(tile_dir):
            for name in files:
                full_name = os.path.join(top, name)
                if self.check(full_name, self.year, self.month):
                    names.append(full_name)
            break
        return names


class JobControl:
    """
    Utilities and some hardcoded stuff for tracking and coding job info.

    :param dict cfg: Configuration for the product we're processing
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def aws_dir(self, item):
        """ Given a prefix like 'LS_WATER_3577_9_-39_20180506102018000000' what is the AWS directory structure?"""

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

    @staticmethod
    def get_gridspec_files(src_dir, src_dir_type, year=None, month=None):
        """
        Extract the NetCDF file list corresponding to 'grid-spec' product for the given year and month
        """

        names = []
        for tile_top, tile_dirs, tile_files in os.walk(src_dir):
            if src_dir_type == 'tiled':
                full_name_list = [os.path.join(tile_top, tile_dir) for tile_dir in tile_dirs]
                with Pool(WORKERS_POOL) as p:
                    names = p.map(TileFiles(year, month).process_tile_files, full_name_list)
                    names = reduce((lambda x, y: x + y), names)
                break
            elif src_dir_type == 'flat':
                names = [os.path.join(tile_top, file) for file in tile_files if TileFiles.check(file, year, month)]
                break
        return names

    @staticmethod
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
        return set(uri[0].split(':')[1].split('#')[0] for uri in files)


class Streamer(object):
    def __init__(self, queue_config, product_name, product_config, queue_dir, job_dir, restart,
                 year=None, month=None, limit=None, file_range=None,
                 reuse_full_list=None, use_datacube=None, datacube_env=None, cog_only=False, upload_only=False):

        def _path_check(file, file_list):
            for item in file_list:
                if os.path.samefile(file, item):
                    return True
            return False

        self.product = product_name
        self.job_control = JobControl(product_config)
        self.queue_dir = queue_dir
        self.dest_url = os.path.join(product_config['bucket'], product_config['aws_dir'])
        self.job_dir = job_dir
        self.cog_only = cog_only
        self.upload_only = upload_only

        # Compute the name of job control files
        job_file = 'streamer_job_control' + '_' + product_name
        job_file = job_file + '_' + str(year) if year else job_file
        job_file = job_file + '_' + str(month) if year and month else job_file
        job_file = job_file + '.log'
        items_all_file = 'items_all' + '_' + product_name
        items_all_file = items_all_file + '_' + str(year) if year else items_all_file
        items_all_file = items_all_file + '_' + str(month) if year and month else items_all_file
        items_all_file = items_all_file + '.log'

        # if restart clear streamer_job_control log and items_all log
        job_file = os.path.join(self.job_dir, job_file)
        items_all_file = os.path.join(self.job_dir, items_all_file)

        if restart:
            if os.path.exists(job_file):
                run_command(['rm', job_file])
            if os.path.exists(items_all_file):
                run_command(['rm', items_all_file])

        # If reuse_full_list, items_all are read from a file if present
        # and save into a file if items are computed new
        if reuse_full_list:
            if os.path.exists(items_all_file):
                with open(items_all_file) as f:
                    items_all = f.read().splitlines()
            else:
                if use_datacube:
                    items_all = JobControl.get_indexed_files(product_name, year, month, datacube_env)
                else:
                    items_all = JobControl.get_gridspec_files(product_config['src_dir'],
                                                              product_config['src_dir_type'], year, month)
                with open(items_all_file, 'a') as f:
                    for item in items_all:
                        f.write(item + '\n')
        else:
            if use_datacube:
                items_all = JobControl.get_indexed_files(product_name, year, month, datacube_env)
            else:
                items_all = JobControl.get_gridspec_files(product_config['src_dir'],
                                                          product_config['src_dir_type'], year, month)

        if file_range:
            start_file, end_file = file_range
            # start_file and end_file are inclusive so we need end_file + 1
            self.items = items_all[start_file: end_file + 1]

            # We need the file system queue specific for this run
            self.message_queue = os.path.join(self.queue_dir,
                                              product_name + '_range_run_{}_{}'.format(start_file, end_file))
            self.queue_dir = os.path.join(self.queue_dir,
                                          product_name + '_range_run_{}_{}'.format(start_file, end_file))
        else:
            # Compute file list
            items_done = []
            if os.path.exists(job_file):
                with open(job_file) as f:
                    items_done = f.read().splitlines()

            self.items = [item for item in items_all if not _path_check(item, items_done)]

            # Enforce if limit
            if limit:
                self.items = self.items[0:limit]

            # We don't want queue to have conflicts with other runs
            self.message_queue = os.path.join(self.queue_dir, product_name + '_single_run')
            self.queue_dir = os.path.join(self.queue_dir, product_name + '_single_run')

        # We are going to start with a empty queue_dir if not upload_only
        if not upload_only:
            if not os.path.exists(self.queue_dir):
                run_command(['mkdir', self.queue_dir])
            else:
                run('rm -fR ' + os.path.join(self.queue_dir, '*'),
                    stderr=subprocess.STDOUT, check=True, shell=True)

            print(self.items.__str__() + ' to do')

        self.job_file = job_file

    def get_queue(self):
        # Cannot use pg bouncer: so use the port 5432
        # thread_pool = psycopg2.pool.ThreadedConnectionPool(1, 2,
        #                                                    user=self.cfg['queue_db']['user'],
        #                                                    host=self.cfg['queue_db']['host'],
        #                                                    port='5432',
        #                                                    database=self.cfg['queue_db']['db_name'])
        connection = connect(dbname='aj9439_db', host='agdcdev-db.nci.org.au', user='aj9439', port='6432')
        cur = connection.cursor()
        cur.execute("""DEALLOCATE PREPARE ALL""")

        pq = PQ(conn=connection)
        try:
            pq.create()
        except ProgrammingError as exc:
            # We ignore a duplicate table error.
            if exc.pgcode != '42P07':
                raise

        return pq[basename(self.message_queue)]

    def compute(self, executor):
        """ The function that runs in the COG conversion thread """
        processed_queue = self.get_queue()
        processed_queue.clear()

        while self.items:
            try:
                queue_capacity = MAX_QUEUE_SIZE - len(processed_queue)
            except OperationalError:
                processed_queue = self.get_queue()
                queue_capacity = MAX_QUEUE_SIZE - len(processed_queue)
            run_size = queue_capacity if len(self.items) > queue_capacity else len(self.items)
            futures = [executor.submit(COGNetCDF.datasets_to_cog, self.product, self.job_control, self.items.pop(),
                                       self.queue_dir) for _ in range(run_size)]
            for future in as_completed(futures):
                file_done = future.result()
                try:
                    processed_queue.put(file_done)
                except OperationalError:
                    processed_queue = self.get_queue()
                    processed_queue.put(file_done)
        try:
            processed_queue.put('Done')
        except OperationalError as exc:
            processed_queue = self.get_queue()
            processed_queue.put('Done')
        print('cog conversion done')

    def upload(self, executor):
        """ The function that run in the file upload to AWS thread """
        processed_queue = self.get_queue()

        while True:
            try:
                queue_size = len(processed_queue)
            except (OperationalError, ProgrammingError):
                processed_queue = self.get_queue()
                queue_size = len(processed_queue)
            items_todo = []
            items_num = 0
            while items_num < queue_size:
                try:
                    new_item = processed_queue.get().data
                    items_todo.append(new_item)
                    items_num += 1
                except OperationalError:
                    processed_queue = self.get_queue()

            futures = []
            while items_todo:
                # We need to pop from the front
                item = items_todo.pop(0)
                if item == 'Done':
                    wait(futures)
                    return
                futures.append(executor.submit(upload_to_s3, self.product, self.job_control, item,
                                               self.queue_dir, self.dest_url, self.job_file))
            wait(futures)

    def run(self):
        with ProcessPoolExecutor(max_workers=WORKERS_POOL) as executor:
            if self.cog_only:
                self.compute(executor)
            elif self.upload_only:
                self.upload(executor)
                # We will remove the queues
                run('rm -rf ' + self.queue_dir, stderr=subprocess.STDOUT, check=True, shell=True)
            else:
                producer = threading.Thread(target=self.compute, args=(executor,))
                consumer = threading.Thread(target=self.upload, args=(executor,))
                producer.start()
                consumer.start()
                producer.join()
                consumer.join()
                # We will remove the queues
                run('rm -rf ' + self.queue_dir, stderr=subprocess.STDOUT, check=True, shell=True)


class Uploader:
    def __init__(self, config):
        self.config = config
        self.queue = Queue()

    def upload(self, executor):
        """ The function that run in the file upload to AWS thread """
        processed_queue = self.queue

        while True:
            try:
                queue_size = len(processed_queue)
            except (OperationalError, ProgrammingError):
                processed_queue = self.get_queue()
                queue_size = len(processed_queue)

            items_todo = []
            items_num = 0
            while items_num < queue_size:
                try:
                    new_item = processed_queue.get().data
                    items_todo.append(new_item)
                    items_num += 1
                except OperationalError:
                    processed_queue = self.get_queue()

            futures = []
            while items_todo:
                # We need to pop from the front
                item = items_todo.pop(0)
                if item == 'Done':
                    wait(futures)
                    return
                futures.append(executor.submit(upload_to_s3, self.product, self.job_control, item,
                                               self.queue_dir, self.dest_url, self.job_file))
            wait(futures)


@click.command()
@click.option('--config', '-c', help="Config file")
@click.option('--product', '-p', required=True, help="Product name")
@click.option('--queue', '-q', required=True, help="Queue directory")
@click.option('--job', '-j', required=True, help="Job directory that store job tracking info")
@click.option('--restart', is_flag=True, help="Restarts the job ignoring prior work")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--limit', '-l', type=int, help="Number of files to be processed in this run")
@click.option('--file_range', '-f', nargs=2, type=int,
              help="The range of files (ends inclusive) with respect to full list")
@click.option('--reuse_full_list', is_flag=True,
              help="Reuse the full file list for the signature(product, year, month)")
@click.option('--use_datacube', is_flag=True, help="Use datacube to extract the list of files")
@click.option('--datacube_env', '-e', help="Specifies the datacube environment")
@click.option('--cog_only', is_flag=True, help="Only run COG conversion")
@click.option('--upload_only', is_flag=True, help="Only run AWS uploads")
def main(config, product, queue, job, restart, year, month, limit, file_range,
         reuse_full_list, use_datacube, datacube_env, cog_only, upload_only):
    assert not (cog_only and upload_only), "Only one of cog_only or upload_only can be used"

    if datacube_env:
        assert use_datacube, "datacube_env must be used with use_datacube option"

    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    queue_config = cfg['queue_db']
    product_config = cfg['products'][product]

    restart_ = True if restart else False
    streamer = Streamer(queue_config, product, product_config, queue, job, restart_, year, month, limit,
                        file_range,
                        reuse_full_list, use_datacube, datacube_env, cog_only, upload_only)
    streamer.run()


def upload():
    pass


if __name__ == '__main__':
    main()

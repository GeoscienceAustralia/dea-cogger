import threading
from concurrent.futures import ProcessPoolExecutor, wait
from multiprocessing import Pool, Queue
import click
import os
from os.path import join as pjoin, basename, dirname, exists
import tempfile
import subprocess
import glob
from subprocess import check_call
from netCDF4 import Dataset
from datetime import datetime
from pandas import to_datetime
import gdal
import xarray
import yaml
from yaml import CLoader as Loader, CDumper as Dumper
from functools import reduce
import logging

MAX_QUEUE_SIZE = 32
WORKERS_POOL = 8


def run_command(command, work_dir):
    """
    Author: Harshu Rampur
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=work_dir)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def upload_to_s3(file, src, dest, job_file):
    file_names = JobControl.get_unstacked_names(file)
    for index in range(len(file_names)):
        prefix = file_names[index]
        item_dir = JobControl.aws_dir(prefix)
        dest_name = os.path.join(dest, item_dir)
        aws_copy = [
            'aws',
            's3',
            'sync',
            src,
            dest_name,
            '--exclude',
            '*',
            '--include',
            '{}*'.format(prefix)
        ]
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                run_command(aws_copy, tmpdir)
        except Exception as e:
            print(e)

        # Remove the file from the queue directory
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                run_command(['rm', '--'] + glob.glob('{}*'.format(os.path.join(src, prefix))), tmpdir)
        except Exception as e:
            print(e.__str__() + os.path.join(src, prefix))

    # job control logs
    with open(job_file, 'a') as f:
        f.write(file + '\n')


class COGNetCDF(object):
    def __init__(self):
        pass

    @staticmethod
    def _dataset_to_yaml(prefix, dataset, dest_dir):
        """ Refactored from Author Harshu Rampur's cog conversion scripts - Write the datasets to separate yaml files"""
        y_fname = os.path.join(dest_dir, prefix + '.yaml')
        dataset_object = dataset.decode('utf-8')
        dataset = yaml.load(dataset_object, Loader=Loader)

        # Update band urls
        for key, value in dataset['image']['bands'].items():
            value['layer'] = '1'
            value['path'] = prefix + '_' + key + '.tif'

        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}
        with open(y_fname, 'w') as fp:
            yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
            logging.info("Writing dataset Yaml to %s", basename(y_fname))

    @staticmethod
    def _dataset_to_cog(prefix, subdatasets, num, dest_dir):
        """ Refactored from Author Harshu Rampur's cog conversion scripts - Write the datasets to separate cog files"""

        with tempfile.TemporaryDirectory() as tmpdir:
            for dts in subdatasets[:-1]:
                band_name = (dts[0].split(':'))[-1]
                out_fname = prefix + '_' + band_name + '.tif'
                env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
                       'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
                subprocess.check_call(env, shell=True)

                # copy to a tempfolder
                temp_fname = pjoin(tmpdir, basename(out_fname))
                to_cogtif = [
                    'gdal_translate',
                    '-of',
                    'GTIFF',
                    '-b',
                    str(num),
                    dts[0],
                    temp_fname]
                run_command(to_cogtif, tmpdir)

                # Add Overviews
                # gdaladdo - Builds or rebuilds overview images.
                # 2, 4, 8,16,32 are levels which is a list of integral overview levels to build.
                add_ovr = [
                    'gdaladdo',
                    '-r',
                    'average',
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

    @staticmethod
    def datasets_to_cog(file, dest_dir):

        file_names = JobControl.get_unstacked_names(file)
        dataset_array = xarray.open_dataset(file)
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()
        for index in range(len(file_names)):
            prefix = file_names[index]
            dataset_item = dataset_array.dataset.item(index)
            COGNetCDF._dataset_to_yaml(prefix, dataset_item, dest_dir)
            COGNetCDF._dataset_to_cog(prefix, subdatasets, index + 1, dest_dir)


class ProcessTile:
    def __init__(self, year=None, month=None):
        self.year = year
        self.month = month

    def process_tile_files(self, tile_dir):
        names = []
        for top, dirs, files in os.walk(tile_dir):
            for name in files:
                name_ = os.path.splitext(name)
                if name_[1] == '.nc':
                    full_name = os.path.join(top, name)
                    time_stamp = name_[0].split('_')[-2]
                    if self.year:
                        if int(time_stamp[0:4]) == self.year:
                            if self.month and len(time_stamp) >= 6:
                                if int(time_stamp[4:6]) == self.month:
                                    names.append(full_name)
                            else:
                                names.append(full_name)
                    else:
                        names.append(full_name)
            break
        return names


class JobControl(object):
    def __init__(self, aws_top_level=None):
        self.aws_top_level = aws_top_level

    @staticmethod
    def wofs_src_dir():
        return '/g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf'

    @staticmethod
    def fc_ls5_src_dir():
        return '/g/data/fk4/datacube/002/FC/LS5_TM_FC'

    @staticmethod
    def fc_ls8_src_dir():
        return '/g/data/fk4/datacube/002/FC/LS8_OLI_FC'

    @staticmethod
    def wofs_aws_top_level():
        return 'WOfS/WOFLs/v2.1.0/combined'

    @staticmethod
    def fc_ls5_aws_top_level():
        return 'fractional-cover/fc/v2.2.0/ls5'

    @staticmethod
    def fc_ls8_aws_top_level():
        return 'fractional-cover/fc/v2.2.0/ls8'

    @staticmethod
    def aws_dir(item):
        item_parts = item.split('_')
        time_stamp = item_parts[-1]
        assert len(time_stamp) == 20, '{} does not have an acceptable timestamp'.format(item)
        year = time_stamp[0:4]
        month = time_stamp[4:6]
        day = time_stamp[6:8]

        y_index = item_parts[-2]
        x_index = item_parts[-3]
        return os.path.join('x_' + x_index, 'y_' + y_index, year, month, day)

    @staticmethod
    def get_unstacked_names(netcdf_file, year=None, month=None):
        """
        warning: hard coded assumptions about FC file names here
        :param netcdf_file: full pathname to a NetCDF file
        :param year:
        :param month:
        :return:
        """
        dts = Dataset(netcdf_file)
        file_id = os.path.splitext(basename(netcdf_file))[0]
        prefix = "_".join(file_id.split('_')[0:-2])
        stack_info = file_id.split('_')[-1]

        dts_times = dts.variables['time']
        names = []
        for index, dt in enumerate(dts_times):
            dt_ = datetime.fromtimestamp(dt)
            time_stamp = to_datetime(dt_).strftime('%Y%m%d%H%M%S%f')
            if year:
                if month:
                    if dt_.year == year and dt_.month == month:
                        names.append('{}_{}'.format(prefix, time_stamp))
                elif dt_.year == year:
                    names.append('{}_{}'.format(prefix, time_stamp))
            else:
                names.append('{}_{}'.format(prefix, time_stamp))
        return names

    @staticmethod
    def get_gridspec_files(src_dir, year=None, month=None):
        """
        warning: hard coded assumptions about FC file names here
        :param src_dir:
        :param year:
        :param month:
        :return:
        """

        names = []
        for tile_top, tile_dirs, tile_files in os.walk(src_dir):
            full_name_list = [os.path.join(tile_top, tile_dir) for tile_dir in tile_dirs]
            with Pool(8) as p:
                names = p.map(ProcessTile(year, month).process_tile_files, full_name_list)
            break
        return reduce((lambda x, y: x + y), names)


class Streamer(object):
    def __init__(self, product, src_dir, queue_dir, bucket_url, job_dir, restart, year=None, month=None):
        self.src_dir = src_dir
        self.queue_dir = queue_dir

        # We are going to start with a empty queue_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            run_command(['rm', '-rf', os.path.join(self.queue_dir, '*')], tmpdir)

        self.dest_url = bucket_url
        self.job_dir = job_dir

        # Compute the name of job control file
        job_file = 'streamer_job_control' + '_' + product
        job_file = job_file + '_' + str(year) if year else job_file
        job_file = job_file + '_' + str(month) if year and month else job_file
        job_file = job_file + '.log'

        # if restart clear streamer_job_control.log
        job_file = os.path.join(self.job_dir, job_file)
        if restart and os.path.exists(job_file):
            with tempfile.TemporaryDirectory() as tmpdir:
                run_command(['rm', job_file], tmpdir)

        # Compute file list
        items_done = []
        if os.path.exists(job_file):
            with open(job_file) as f:
                items_done = f.read().splitlines()

        items_all = JobControl.get_gridspec_files(self.src_dir, year, month)
        self.items = [item for item in items_all if item not in items_done]
        self.items.sort(reverse=True)
        print(self.items.__str__() + ' to do')
        self.job_file = job_file

    def compute(self, processed_queue):
        with ProcessPoolExecutor(max_workers=WORKERS_POOL) as executor:
            while self.items:
                queue_capacity = MAX_QUEUE_SIZE - processed_queue.qsize()
                run_size = queue_capacity if len(self.items) > queue_capacity else len(self.items)
                futures = []
                items = []
                for i in range(run_size):
                    items.append(self.items.pop())
                    futures.append(executor.submit(COGNetCDF.datasets_to_cog,
                                                   items[i], self.queue_dir))
                wait(futures)
                for i in range(run_size):
                    processed_queue.put(items[i])
        processed_queue.put(None)

    def upload(self, processed_queue):
        while True:
            item = processed_queue.get(block=True, timeout=None)
            if item is None:
                break
            upload_to_s3(item, self.queue_dir, self.dest_url, self.job_file)

    def run(self):
        processed_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        producer = threading.Thread(target=self.compute, args=(processed_queue,))
        consumer = threading.Thread(target=self.upload, args=(processed_queue,))
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()


@click.command()
@click.option('--product', '-p', required=True, help="Product name: one of fc-ls5, fc-ls8, or wofs")
@click.option('--queue', '-q', required=True, help="Queue directory")
@click.option('--bucket', '-b', required=True, help="Destination Bucket Url")
@click.option('--job', '-j', required=True, help="Job directory that store job tracking info")
@click.option('--restart', is_flag=True, help="Restarts the job ignoring prior work")
@click.option('--year', '-y', type=click.INT, help="The year")
@click.option('--month', '-m', type=click.INT, help="The month")
@click.option('--src', '-s',type=click.Path(exists=True), help="Source directory just above tiles directories")
def main(product, queue, bucket, job, restart, year, month, src):
    assert product in ['fc-ls5', 'fc-ls8', 'wofs'], "Product name must be one of fc-ls5, fc-ls8, or wofs"

    src_dir = None
    bucket_url = None
    if product == 'fc-ls5':
        src_dir = JobControl.fc_ls5_src_dir()
        bucket_url = os.path.join(bucket, JobControl.fc_ls5_aws_top_level())
    elif product == 'fc-ls8':
        src_dir = JobControl.fc_ls8_src_dir()
        bucket_url = os.path.join(bucket, JobControl.fc_ls8_aws_top_level())
    elif product == 'wofs':
        src_dir = JobControl.wofs_src_dir()
        bucket_url = os.path.join(bucket, JobControl.wofs_aws_top_level())

    if src:
        src_dir = src
    restart_ = True if restart else False
    streamer = Streamer(product, src_dir, queue, bucket_url, job, restart_, year, month)
    streamer.run()


if __name__ == '__main__':
    main()

import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
import queue
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

MAX_QUEUE_SIZE = 12
WORKERS_POOL = 2


def run_command(command, work_dir):
    """
    Author: Harshu Rampur
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=work_dir)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def check_dir(fname):
    file_name = fname.split('/')
    rel_path = pjoin(*file_name[-2:])
    return rel_path


# def getfilename(fname, outdir):
#     """ To create a temporary filename to add overviews and convert to COG
#         and create a file name just as source but without '.TIF' extension
#     """
#     rel_path = check_dir(fname)
#     out_fname = pjoin(outdir, rel_path)
#
#     if not exists(dirname(out_fname)):
#         os.makedirs(dirname(out_fname))
#     return out_fname


def geotiff_to_cog(fname, src, dest, message_queue=None):
    """ Author: Harshu Rampur (Adapted)
        Convert the Geotiff to COG using gdal commands
        Blocksize is 512
        TILED <boolean>: Switch to tiled format
        COPY_SRC_OVERVIEWS <boolean>: Force copy of overviews of source dataset
        COMPRESS=[NONE/DEFLATE]: Set the compression to use. DEFLATE is only available if NetCDF has been compiled with
                  NetCDF-4 support. NC4C format is the default if DEFLATE compression is used.
        ZLEVEL=[1-9]: Set the level of compression when using DEFLATE compression. A value of 9 is best,
                      and 1 is least compression. The default is 1, which offers the best time/compression ratio.
        BLOCKXSIZE <int>: Tile Width
        BLOCKYSIZE <int>: Tile/Strip Height
        PREDICTOR <int>: Predictor Type (1=default, 2=horizontal differencing, 3=floating point prediction)
        PROFILE <string-select>: possible values: GDALGeoTIFF,GeoTIFF,BASELINE,
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        fname_full = pjoin(src, fname)
        temp_fname = pjoin(tmpdir, fname)
        out_fname = pjoin(dest, fname)

        env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
               'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
        subprocess.check_call(env, shell=True)

        # copy to a tempfolder
        to_cogtif = [
            'gdal_translate',
            '-of',
            'GTIFF',
            fname_full,
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
            'PREDICTOR=1',
            '-co',
            'PROFILE=GeoTIFF',
            temp_fname,
            out_fname]
        run_command(cogtif, dest)
        if message_queue:
            message_queue.put(fname, block=True)


def process_file(file, src, dest, message_queue=None):
    geotiff_to_cog(file, src, dest, message_queue)


def upload_to_s3(item, src, dest, job_file):
    item_dir = JobControl.fc_aws_dir(item)
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
        '{}*'.format(item)
    ]
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_command(aws_copy, tmpdir)
    except Exception as e:
        print(e)

    # job control logs
    with open(job_file, 'a') as f:
        f.write(item + '\n')

    # Remove the file from the queue directory
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_command(['rm', '--'] + glob.glob('{}*'.format(os.path.join(src, item))), tmpdir)
    except Exception as e:
        print(e)


class COGWriter(object):
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
    def datasets_to_yaml(file, dest_dir):

        file_names = JobControl.get_unstacked_names(file)
        dataset_array = xarray.open_dataset(file)
        for count in range(len(file_names)):
            prefix, _, _ = file_names[count]
            dataset_object = dataset_array.dataset.item(count)
            COGWriter._dataset_to_yaml(prefix, dataset_object, dest_dir)

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
    def extract_to_cog(file, index, dest_dir):

        file_names = JobControl.get_unstacked_names(file)
        prefix, _, _ = file_names[index]
        dataset_array = xarray.open_dataset(file)
        dataset_item = dataset_array.dataset.item(index)
        COGWriter._dataset_to_yaml(prefix, dataset_item, dest_dir)
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()
        COGWriter._dataset_to_cog(prefix, subdatasets, index + 1, dest_dir)

    @staticmethod
    def datasets_to_cog(file, dest_dir, message_queue=None):

        file_names = JobControl.get_unstacked_names(file)
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        results = []
        with Pool(processes=8) as pool:
            for index in range(len(file_names)):
                prefix, _, _ = file_names[index]
                results.append(pool.apply_async(func=COGWriter.extract_to_cog, args=(file, index, dest_dir),
                                                callback=lambda res: print(res),
                                                error_callback=lambda e: print(e)))
            for index in range(len(file_names)):
                prefix, _, _ = file_names[index]
                results[index].wait()
                if message_queue:
                    message_queue.put(prefix, block=True)
                print(prefix + ' added to queue')


class ProcessTile:
    def __init__(self, year=None, month=None):
        self.year = year
        self.month = month

    def process_tile_names(self, tile_dir):
        names = []
        for top, dirs, files in os.walk(tile_dir):
            for name in files:
                name_ = os.path.splitext(name)
                if name_[1] == '.nc':
                    full_name = os.path.join(top, name)
                    time_stamp = name_[0].split('_')[-2]
                    if self.year:
                        if int(time_stamp[0:4]) == self.year:
                            if self.month:
                                if int(time_stamp[4:6]) == self.month:
                                    names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
                            else:
                                names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
                    else:
                        names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
            break
        return names


class JobControl(object):
    def __init__(self, aws_top_level=None):
        self.aws_top_level = aws_top_level

    @staticmethod
    def fc_aws_dir(item, aws_top_level=None):
        item_parts = item.split('_')
        time_stamp = item_parts[-1]
        assert len(time_stamp) == 20, '{} does have an acceptable timestamp'.format(item)
        year = time_stamp[0:4]
        month = time_stamp[4:6]
        day = time_stamp[6:8]

        y_index = item_parts[-2]
        x_index = item_parts[-3]

        product = item_parts[0].lower()
        if aws_top_level:
            return os.path.join(aws_top_level, product, 'x_' + x_index, 'y_' + y_index, year, month, day)
        else:
            return os.path.join(product, 'x_' + x_index, 'y_' + y_index, year, month, day)

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
                        names.append(('{}_{}'.format(prefix, time_stamp), index, netcdf_file))
                elif dt_.year == year:
                    names.append(('{}_{}'.format(prefix, time_stamp), index, netcdf_file))
            else:
                names.append(('{}_{}'.format(prefix, time_stamp), index, netcdf_file))
        return names

    @staticmethod
    def get_unstacked_files(src_dir, year=None, month=None):
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
                names = p.map(ProcessTile(year, month).process_tile_names, full_name_list)
            break
        return reduce((lambda x, y: x + y), names)

    def get_dataset_names(self, src_dir, year=None, month=None):
        """
        warning: hard coded assumptions about FC file names here
        :param src_dir:
        :param year:
        :param month:
        :return:
        """
        names = []
        for tile_top, tile_dirs, tile_files in os.walk(src_dir):
            for tile_dir in tile_dirs:
                for top, dirs, files in os.walk(os.path.join(tile_top, tile_dir)):
                    for name in files:
                        name_ = os.path.splitext(name)
                        if name_[1] == '.nc':
                            full_name = os.path.join(top, name)
                            time_stamp = name_[0].split('_')[-2]
                            # Is it a stacked NetCDF file
                            if len(Dataset(full_name).variables['time']) > 1:
                                if year:
                                    if int(time_stamp[0:4]) == year:
                                        names.extend(self.get_unstacked_names(full_name, year, month))
                                else:
                                    names.extend(self.get_unstacked_names(full_name))
                            else:
                                if year:
                                    if int(time_stamp[0:4]) == year:
                                        if month:
                                            if int(time_stamp[4:6]) == month:
                                                names.append(("_".join(name_[0].split('_')[0:-1]), None, full_name))
                                        else:
                                            names.append(("_".join(name_[0].split('_')[0:-1]), None, full_name))
                                else:
                                    names.append(("_".join(name_[0].split('_')[0:-1]), None, full_name))
        return names


class Streamer(object):
    def __init__(self, src_dir, queue_dir, dest_url, job_dir, restart):
        self.src_dir = src_dir
        self.queue_dir = queue_dir

        # We are going to start with a empty queue_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            run_command(['rm', '-rf', os.path.join(self.queue_dir, '*')], tmpdir)

        self.dest_url = dest_url
        self.job_dir = job_dir

        # if restart clear streamer_job_control.log
        job_file = os.path.join(self.job_dir, 'streamer_job_control.log')
        if restart and os.path.exists(job_file):
            with tempfile.TemporaryDirectory() as tmpdir:
                run_command(['rm', job_file], tmpdir)

        # Compute file list
        items_done = []
        if os.path.exists(job_file):
            with open(job_file) as f:
                items_done = f.read().splitlines()

        items_all = os.listdir(self.src_dir)
        self.items = [item for item in items_all if item not in items_done]
        self.items.sort(reverse=True)
        print(self.items.__str__() + ' to do')
        self.job_file = job_file

    def compute(self, processed_queue):
        while self.items:
            if len(self.items) >= WORKERS_POOL and (MAX_QUEUE_SIZE - processed_queue.qsize() >= 0):
                # Speed-up processing with threads
                with ThreadPoolExecutor(max_workers=WORKERS_POOL) as executor:
                    futures = []
                    for i in range(WORKERS_POOL):
                        futures.append(executor.submit(COGWriter.datasets_to_cog,
                                                       os.path.join(self.src_dir, self.items.pop()),
                                                       self.queue_dir,
                                                       processed_queue))
            else:
                COGWriter.datasets_to_cog(os.path.join(self.src_dir, self.items.pop()),
                                          self.queue_dir, processed_queue)
        # Signal end of processing
        processed_queue.put(None)

    def upload(self, processed_queue):
        while True:
            item = processed_queue.get(block=True, timeout=None)
            if item is None:
                break
            upload_to_s3(item, self.queue_dir, self.dest_url, self.job_file)

    def run(self):
        processed_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        producer = threading.Thread(target=self.compute, args=(processed_queue,))
        consumer = threading.Thread(target=self.upload, args=(processed_queue,))
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()


@click.command()
@click.option('--queue', '-q', required=True, help="Queue directory")
@click.option('--dest', '-d', required=True, help="Destination Url")
@click.option('--job', '-j', required=True, help="Job directory that store job tracking info")
@click.option('--restart', is_flag=True, help="Restarts the job ignoring prior work")
@click.argument('src', type=click.Path(exists=True))
def main(queue, dest, job, restart, src):
    restart_ = True if restart else False
    streamer = Streamer(src, queue, dest, job, restart_)
    streamer.run()


if __name__ == '__main__':
    main()

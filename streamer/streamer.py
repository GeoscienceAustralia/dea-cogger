import threading
from concurrent.futures import ThreadPoolExecutor
import queue
import click
import os
from os.path import join as pjoin, basename, dirname, exists
import tempfile
import subprocess
from subprocess import check_call
from netCDF4 import Dataset
from datetime import datetime
from pandas import to_datetime
import gdal
import xarray
import yaml
from yaml import CLoader as Loader, CDumper as Dumper
import logging

MAX_QUEUE_SIZE = 4
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


def geotiff_to_cog(fname, src, dest):
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


def process_file(file, src, dest):
    geotiff_to_cog(file, src, dest)


def upload_to_s3(item, src, dest, job_file):
    src_name = os.path.join(src, item)

    dest_name = os.path.join(dest, item)
    aws_copy = [
        'aws',
        's3',
        'cp',
        src_name,
        dest_name
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        run_command(aws_copy, tmpdir)

    # job control logs
    with open(job_file, 'a') as f:
        f.write(item + '\n')

    # Remove the file from the queue directory
    with tempfile.TemporaryDirectory() as tmpdir:
        run_command(['rm', src_name], tmpdir)


class COGWriter(object):
    def __init__(self):
        pass

    @staticmethod
    def _dataset_to_yaml(prefix, dataset, dest_dir):
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
        """ Refactored from Author Harshu Rampur's cog conversion scripts - Write the datasets to separate yaml files"""

        file_names = JobControl.get_unstacked_names(file)
        dataset_array = xarray.open_dataset(file)
        for count in range(len(file_names)):
            prefix, _, _ = file_names[count]
            dataset_object = dataset_array.dataset.item(count)
            COGWriter._dataset_to_yaml(prefix, dataset_object, dest_dir)

    @staticmethod
    def _dataset_to_cog(prefix, subdatasets, index, dest_dir):
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
                    str(index),
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
    def datasets_to_cog(file, dest_dir, message_queue=None):
        """ Refactored from Author Harshu Rampur's cog conversion scripts - Write the datasets to separate cog files"""

        file_names = JobControl.get_unstacked_names(file)
        dataset = gdal.Open(file, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()
        dataset_array = xarray.open_dataset(file)
        for count in range(1, len(file_names) + 1):
            prefix, _, _ = file_names[count - 1]
            COGWriter._dataset_to_yaml(prefix, dataset_array.dataset.item(count - 1), dest_dir)
            COGWriter._dataset_to_cog(prefix, subdatasets, count, dest_dir)
            if message_queue:
                message_queue.put(prefix, block=True)


class JobControl(object):
    def __init__(self):
        pass

    def aws_dir(self, file_name, aws_top_level: None):
        """
        This currently tested only for FC ls5, ls8
        :param file_name: base NetCDF file name without directory structure,
                          e.g. 'LS5_TM_FC_3577_-8_-22_1997_v20171127040509.nc'
        :param aws_top_level: AWS specific top level part of the directory,
                              e.g. /fractional_cover/fc/v2.2.0
        :return: directory structure,
                 e.g. ls5/x_-8/y_-22/1997 for the above LS5 file
        """
        pass

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

    def get_dataset_names(self, src_dir, year=None, month=None):
        """
        warning: hard coded assumptions about FC file names here
        :param src_dir:
        :param year:
        :param month:
        :return:
        """
        names = []
        for top, dirs, files in os.walk(src_dir):
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
                                        names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
                                else:
                                    names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
                        else:
                            names.append(("_".join(name_[0].split('_')[0:-1]), full_name))
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
        self.job_file = job_file

    def compute(self, processed_queue):
        while self.items:
            if len(self.items) >= WORKERS_POOL and (MAX_QUEUE_SIZE - processed_queue.qsize() >= 0):
                # Speed-up processing with threads
                with ThreadPoolExecutor(max_workers=WORKERS_POOL) as executor:
                    futures = []
                    items = []
                    for i in range(WORKERS_POOL):
                        items.append(self.items.pop())
                        futures.append(executor.submit(process_file, items[i], self.src_dir, self.queue_dir))
                    # callbacks behaving strange so try the following
                    for i in range(WORKERS_POOL):
                        futures[i].result()
                        processed_queue.put(items[i])
            elif not processed_queue.full():
                item = self.items.pop()
                process_file(item, self.src_dir, self.queue_dir)
                processed_queue.put(item)
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

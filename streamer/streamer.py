#!/usr/bin/env python
import logging
import os
import re
import sys
import subprocess
from datetime import datetime
from os.path import join as pjoin, basename, exists
from subprocess import check_call
from pandas import Timestamp
from pathlib import Path
import click
import gdal
import numpy as np
import xarray
import yaml
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper
from enum import IntEnum

from datacube import Datacube
from datacube.model import Range
from mpi4py import MPI
from cogeo import cog_translate

LOG = logging.getLogger('cog-converter')
stdout_hdlr = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
        '[%(asctime)s.%(msecs)03d - %(levelname)s] %(message)s')
stdout_hdlr.setFormatter(formatter)
LOG.addHandler(stdout_hdlr)
LOG.setLevel(logging.DEBUG)

DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}
DEFAULT_CONFIG = """
products: 
    fcp_cog:
        src_template: whatever_{x}_{y}_{time}
        dest_template: x_{x}/y_{y}/{year}
        nonpym_list: ["source", "observed"]
        predictor: 2
        default_rsp: average
"""
MPI_COMM = MPI.COMM_WORLD      # Get MPI communicator object
MPI_JOB_SIZE = MPI_COMM.size   # Total number of processes
MPI_JOB_RANK = MPI_COMM.rank   # Rank of this process
MPI_JOB_STATUS = MPI.Status()  # Get MPI status object


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
        check_call(command, stderr=subprocess.STDOUT, cwd=None, env=os.environ)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, nonpym_list=None, default_rsp=None,
                 bands_rsp=None, dest_template=None, src_template=None, predictor=None):
        self.nonpym_list = nonpym_list
        self.black_list = black_list
        self.white_list = white_list
        if predictor is None:
            self.predictor = 2
        else:
            self.predictor = predictor
        if default_rsp is None:
            self.default_rsp = 'average'
        else:
            self.default_rsp = default_rsp
        self.bands_rsp = bands_rsp
        if dest_template is None:
            self.dest_template = "x_{x}/y_{y}/{year}"
        else:
            self.dest_template = dest_template
        if src_template is None:
            self.src_template = "{x}_{y}_{time}"
        else:
            self.src_template = src_template

    def __call__(self, input_fname, dest_dir):
        prefix_name = self._make_out_prefix(input_fname, dest_dir)
        self.netcdf_to_cog(input_fname, prefix_name)

    def _make_out_prefix(self, input_fname, dest_dir):
        abs_fname = basename(input_fname)
        prefix_name = re.search(r"[-\w\d.]*(?=\.\w)", abs_fname).group(0)
        r = re.compile(r"(?<=_)[-\d.]+")
        indices = r.findall(prefix_name)
        r = re.compile(r"(?<={)\w+")
        key_indices = r.findall(self.src_template)
        keys = r.findall(self.dest_template)
        if len(indices) > len(key_indices):
            indices = indices[-len(key_indices):]

        if len(key_indices) > 3:
            indices = indices[-len(key_indices): -(len(key_indices) - 3)]
        else:
            indices += [None] * (3 - len(indices))
            x_index, y_index, date_time = indices

        dest_dict = {keys[0]: x_index, keys[1]: y_index}
        if date_time is not None:
            year = re.search(r"\d{4}", date_time)
            month = re.search(r'(?<=\d{4})\d{2}', date_time)
            day = re.search(r'(?<=\d{6})\d{2}', date_time)
            time = re.search(r'(?<=\d{8})\d+', date_time)
            if year is not None and len(keys) >= 3:
                dest_dict[keys[2]] = year.group(0)
            if month is not None and len(keys) >= 4:
                dest_dict[keys[3]] = month.group(0)
            if day is not None and len(keys) >= 5:
                dest_dict[keys[4]] = day.group(0)
            if time is not None and len(keys) >= 6:
                dest_dict[keys[5]] = time.group(0)
        else:
            self.dest_template = '/'.join(self.dest_template.split('/')[0:2])

        out_dir = pjoin(dest_dir, self.dest_template.format(**dest_dict))
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
        except:
            LOG.info("netcdf error: %s", input_file)
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

    @staticmethod
    def _dataset_to_yaml(prefix, dataset_array: xarray.Dataset, rastercount):
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

            invalid_band = []
            # Update band urls
            for key, value in dataset['image']['bands'].items():
                if self.black_list is not None:
                    if re.search(self.black_list, key) is not None:
                        invalid_band.append(key)
                        continue

                if self.white_list is not None:
                    if re.search(self.white_list, key) is None:
                        invalid_band.append(key)
                        continue

                if rastercount == 1:
                    tif_path = basename(prefix + '_' + key + '.tif')
                else:
                    tif_path = basename(prefix + '_' + key + '_' + str(i + 1) + '.tif')

                value['layer'] = str(i + 1)
                value['path'] = tif_path

            for band in invalid_band:
                dataset['image']['bands'].pop(band)

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

    @staticmethod
    def _check_tif(fname):
        try:
            cog_tif = gdal.Open(fname, gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except:
            return False

        if t_stats > [0.] * 4:
            return True
        else:
            return False


class COGProductConfiguration:
    """
    Utilities and some hardcoded stuff for tracking and coding job info.

    :param dict cfg: Configuration for the product we're processing
    """

    def __init__(self, cfg):
        self.cfg = cfg


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
    def _filename_from_uri(uri):
        return uri[0].split(':')[1].split('#')[0]

    return set(_filename_from_uri(uri) for uri in files)


def check_date(context, param, value):
    """
    Click callback to validate a date string
    """
    try:
        return Timestamp(value)
    except ValueError as error:
        raise ValueError('Date must be valid string for pandas Timestamp') from error


def get_indexed_files(product, year=None, month=None, from_date=None, datacube_env=None):
    """
    Extract the file list corresponding to a product for the given year and month using datacube API.
    """
    query = {'product': product}
    if from_date:
        query['time'] = Range(datetime(year=from_date.year, month=from_date.month, day=from_date.day),
                              datetime.now())
    elif year and month:
        query['time'] = Range(datetime(year=year, month=month, day=1), datetime(year=year, month=month + 1, day=1))
    elif year:
        query['time'] = Range(datetime(year=year, month=1, day=1), datetime(year=year + 1, month=1, day=1))
    dc = Datacube(app='streamer', env=datacube_env)
    files = dc.index.datasets.search_returning(field_names=('uri',), **query)

    # Extract file name from search_result
    def filename_from_uri(uri):
        return uri[0].split('//')[1]

    return set(filename_from_uri(uri) for uri in files)


def netcdf_cog_worker(wargs=None):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format using MPI
    Uses a configuration file to define the file naming schema.
    """
    netcdf_cog_fp = COGNetCDF(**list(wargs)[0])
    netcdf_cog_fp(list(wargs)[1], list(wargs)[2])


def _raise_value_err(exp):
    raise ValueError(exp)


@click.group(help=__doc__)
def cli():
    pass


@cli.command(name='generate-work-list')
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
@click.option('--from-date', callback=check_date, help="The date from which the dataset time")
@click.option('--output_dir', '-o', help='The list will be saved to this directory')
def generate_work_list(product_name, year, month, from_date, output_dir):
    """
    Connect to an ODC database and list NetCDF files
    """

    # get file list
    items_all = get_indexed_files(product_name, year, month, from_date, 'dea-prod')

    out_file = Path(output_dir) / 'file_list'
    with open(out_file, 'w') as fp:
        for item in sorted(items_all):
            fp.write(item + '\n')


@cli.command(name='mpi-convert-cog')
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--numprocs', type=int, help='Number of processes', required=True, default=1)
@click.argument('filelist', nargs=1, required=True)
def mpi_convert_cog(config, output_dir, product, numprocs, filelist):
    """
    Parallelise COG convert using MPI
    Iterate over filename and output dir as job argument
    """
    global MPI_COMM, MPI_JOB_SIZE, MPI_JOB_RANK

    if config:
        with open(config) as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    try:
        with open(filelist) as fb:
            file_list = np.genfromtxt(fb, dtype='str')
        tasks = file_list.size
    except FileNotFoundError:
        LOG.error(f'MPI Worker ({MPI_JOB_RANK}): No netCDF file/s found in the input path')
        raise
    else:
        if tasks == 0:
            _raise_value_err(f'MPI Worker ({MPI_JOB_RANK}): No netCDF file/s found in the input path')

    product_config = cfg['products'][product]
    num_workers = numprocs if numprocs > 0 else _raise_value_err(
        f"MPI Worker ({MPI_JOB_RANK}): Number of processes cannot be zero")

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


if __name__ == '__main__':
    cli()

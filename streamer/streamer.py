#!/usr/bin/env python
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from os.path import join as pjoin, basename, exists
from subprocess import check_call
from time import sleep

import click
import gdal
import numpy as np
import xarray
import yaml
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from datacube import Datacube
from datacube.model import Range
from .cogeo import cog_translate

LOG = logging.getLogger('cog-converter')
WORKERS_POOL = 4

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


def run_command(command):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=None, env=os.environ)
        # run(command, cwd=work_dir, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, nonpym_list=None, default_rsp=None,
                 src_nodata='auto',
                 bands_rsp=None, dest_template=None, src_template=None, predictor=None):
        self.nonpym_list = nonpym_list
        self.black_list = black_list
        self.white_list = white_list
        self.src_nodata = src_nodata
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
        prefix_name = self.make_out_prefix(input_fname, dest_dir)
        self.netcdf_to_cog(input_fname, prefix_name)

    def make_out_prefix(self, input_fname, dest_dir):
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
            re_white = "|".join(self.white_list)
        if self.black_list is not None:
            re_black = "|".join(self.black_list)
        if self.nonpym_list is not None:
            re_nonpym = "|".join(self.nonpym_list)

        rastercount = 0
        for dts in subdatasets[:-1]:
            rastercount = gdal.Open(dts[0]).RasterCount
            for i in range(rastercount):
                band_name = dts[0].split(':')[-1]

                # Only do specified bands if specified
                if self.black_list is not None:
                    if re.search(re_black, band_name) is not None:
                        continue

                if self.white_list is not None:
                    if re.search(re_white, band_name) is None:
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
                    if re.search(re_nonpym, band_name) is not None:
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
                              nodata=self.src_nodata,
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
    def filename_from_uri(uri):
        return uri[0].split(':')[1].split('#')[0]

    return set(filename_from_uri(uri) for uri in files)


@click.group(help=__doc__)
def cli():
    pass


@cli.command()
@click.option('--product-name', '-p', required=True, help="Product name")
@click.option('--year', '-y', type=int, help="The year")
@click.option('--month', '-m', type=int, help="The month")
def generate_work_list(product_name, year, month):
    """
    Connect to an ODC database and list NetCDF files
    """
    items_all = get_indexed_files(product_name, year, month)

    for item in sorted(items_all):
        print(item)


try:
    from mpi4py import MPI
except:
    LOG.warning("mpi4py is not available")


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--flist', '-l', help='List of file names', default=None)
@click.argument('filenames', nargs=-1, type=click.Path())
def convert_cog(config, output_dir, product, flist, filenames):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format

    Uses a configuration file to define the file naming schema.

    """
    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    product_config = cfg['products'][product]

    cog_convert = COGNetCDF(**product_config)

    if flist is not None:
        with open(flist, 'r') as fb:
            file_list = np.genfromtxt(fb, dtype='str')
    else:
        file_list = list(filenames)

    try:
        comm = MPI.Comm.Get_parent()
        size = comm.Get_size()
        rank = comm.Get_rank()
    except:
        LOG.info("Run with single process")
        for filename in file_list:
            cog_convert(filename, output_dir)
    else:
        comm.Merge(True)
        batch_size = int(len(file_list) / size)
        for filename in file_list[rank * batch_size:(rank + 1) * batch_size]:
            cog_convert(filename, output_dir)
        comm.Disconnect()


@cli.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', help='Output directory', required=True)
@click.option('--product', help='Product name', required=True)
@click.option('--numprocs', type=int, help='Number of processes', required=True, default=1)
@click.option('--cog-path', help='cog convert script path', required=True,
              default='../COG-Conversion/streamer/streamer.py')
@click.argument('filelist', nargs=1, required=True)
def mpi_convert_cog(config, output_dir, product, numprocs, cog_path, filelist):
    """
    parallelize the COG convert with MPI.

    """
    cmd_line = [cog_path] + ['convert_cog', '-c'] + [config] + ['--output-dir'] + [output_dir] + ['--product'] + [
        product]
    args = cmd_line
    with open(filelist, 'r') as fb:
        file_list = np.genfromtxt(fb, dtype='str')
    LOG.debug("Process file %s", filelist)
    file_odd = len(file_list) % numprocs
    LOG.debug("file_odd %d", file_odd)
    margs = args + ['-l', filelist]
    while True:
        try:
            comm = MPI.COMM_SELF.Spawn(sys.executable,
                                       args=margs,
                                       maxprocs=numprocs)

        except:
            sleep(1)
        else:
            comm.Merge()
            break
    comm.Disconnect()
    LOG.debug("Batch done")
    if file_odd > 0:
        numprocs = file_odd
        margs = args + list(file_list[-file_odd:])
        while True:
            try:
                comm = MPI.COMM_SELF.Spawn(sys.executable,
                                           args=margs,
                                           maxprocs=numprocs)
            except:
                sleep(1)
            else:
                comm.Merge()
                break
        comm.Disconnect()
    LOG.debug("Job done")


if __name__ == '__main__':
    LOG = logging.getLogger(__name__)
    LOG.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    LOG.addHandler(ch)

    cli()

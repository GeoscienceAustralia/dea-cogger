#!/usr/bin/env python
"""
Batch Convert NetCDF files to Cloud-Optimised-GeoTIFF and upload to S3

This tool is broken into 3 pieces:

 1) Work out the difference between NetCDF files stored locally, and GeoTIFF files in S3
 2) Batch convert NetCDF files into Cloud Optimised GeoTIFFs
 3) Watch a directory and upload files to S3


Finding files to process
------------------------
This can either be done manually with a command like `find <dir> --name '*.nc'`, or
by searching an ODC Index using::

    python streamer.py generate_work_list --product-name <name> [--year <year>] [--month <month>]

This will print a list of NetCDF files which can be piped to `convert_cog`.


Batch Converting NetCDF files
-----------------------------
::

    python streamer.py convert_cog [--max-procs <int>] --config <file> --product <product> --output-dir <dir> \
    List of NetCDF files...

Use the settings in a configuration file to:

- Parse variables from the NetCDF filename/directory
- Generate output directory structure and filenames
- Configure COG Overview resampling method

When run, each `ODC Dataset` in each NetCDF file will be converted into an output directory containing a COG
for each `band`, as well as a `.yaml` dataset definition, and a `upload-destination.txt` file containing
the full destination directory.

During processing, `<output-directory/WORKING/` will contain in-progress Datasets.
Once a Dataset is complete, it will be moved into the `<output-directory>/TO_UPLOAD/`



Uploading to S3
---------------

Watch `<output-directory>/TO_UPLOAD/` for new COG Dataset Directories, and upload them to the `<upload-destination>`.

Once uploaded, directories can either be deleted or moved elsewhere for safe keeping.




Configuration
-------------

The program uses a config, that in particular specify product descriptions such as whether time values are taken
from filename or dateset or there no time associated with datasets, source and destination filename templates,
aws directory, dataset specific aws directory suffix, resampling method for cog conversion.
The destination template must only specify the prefix of the file excluding the band name details and
extension. Examples of such config spec for products are as follows:

    ls5_fc_albers:
        time_taken_from: dataset
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        resampling_method: average
    wofs_filtered_summary:
        time_taken_from: notime
        src_template: wofs_filtered_summary_{x}_{y}.nc
        dest_template: wofs_filtered_summary_{x}_{y}
        src_dir: /g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf
        aws_dir: WOfS/filtered_summary/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}
        local_dir_suffix: '{x}_{y}'
        bands_to_cog_convert: [confidence]
        default_resampling_method: mode
        band_resampling_methods: {confidence: average}
"""
import io
import logging
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from os.path import join as pjoin, basename
from pathlib import Path
from subprocess import run

import click
import gdal
import xarray
import yaml
from netCDF4 import Dataset
from pandas import to_datetime
from parse import parse
from tqdm import tqdm

LOG = logging.getLogger(__name__)

try:
    from yaml import CSafeLoader as Loader, CSafeDumper as Dumper
except ImportError:
    LOG.warn('Unable to import the Optimised libyaml parser/dumper. YAML performance will be degraded.')
    from yaml import SafeLoader as Loader, SafeDumper as Dumper

WORKERS_POOL = 4

DEFAULT_CONFIG = """
products: 
    wofs_albers: 
        time_taken_from: filename
        src_template: LS_WATER_3577_{x}_{y}_{time}_v{}.nc 
        dest_template: LS_WATER_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf
        aws_dir: WOfS/WOFLs/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: mode
    wofs_filtered_summary:
        time_taken_from: notime
        src_template: wofs_filtered_summary_{x}_{y}.nc
        dest_template: wofs_filtered_summary_{x}_{y}
        src_dir: /g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf
        aws_dir: WOfS/filtered_summary/v2.1.0/combined
        aws_dir_suffix: x_{x}/y_{y}
        local_dir_suffix: '{x}_{y}'
        bands_to_cog_convert: [confidence]
        default_resampling_method: mode
        band_resampling_methods: {confidence: average}
    wofs_annual_summary:
        time_taken_from: filename
        src_template: WOFS_3577_{x}_{y}_{time}_summary.nc
        dest_template: WOFS_3577_{x}_{y}_{time}_summary
        src_dir: /g/data/fk4/datacube/002/WOfS/WOfS_Stats_Ann_25_2_1/netcdf
        aws_dir: WOfS/annual_summary/v2.1.5/combined
        aws_dir_suffix: x_{x}/y_{y}/{year}
        bucket: s3://dea-public-data-dev
        default_resampling_method: mode
    ls5_fc_albers:
        time_taken_from: dataset
        src_template: LS5_TM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS5_TM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS5_TM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls5
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
    ls7_fc_albers:
        time_taken_from: dataset
        src_template: LS7_ETM_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS7_ETM_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS7_ETM_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls7
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
    ls8_fc_albers:
        time_taken_from: dataset
        src_template: LS8_OLI_FC_3577_{x}_{y}_{time}_v{}.nc
        dest_template: LS8_OLI_FC_3577_{x}_{y}_{time}
        src_dir: /g/data/fk4/datacube/002/FC/LS8_OLI_FC
        aws_dir: fractional-cover/fc/v2.2.0/ls8
        aws_dir_suffix: x_{x}/y_{y}/{year}/{month}/{day}
        default_resampling_method: average
"""


def run_command(command, work_dir=None):
    """
    A simple utility to execute a subprocess command.
    """
    try:
        run(command, cwd=work_dir, check=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' failed with error (code {}): {}".format(e.cmd, e.returncode, e.output))


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
        dest_dir = Path(dest_dir)
        prefix_names = product_config.get_unstacked_names(input_file)

        dataset_array = xarray.open_dataset(input_file)
        dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()

        generated_datasets = {}

        for index, prefix in enumerate(prefix_names):
            prefix = prefix_names[index]
            dest = dest_dir / prefix
            dest.mkdir(exist_ok=True)

            # Read the Dataset Metadata from the 'dataset' variable in the NetCDF file, and save as YAML
            dataset_item = dataset_array.dataset.item(index)
            COGNetCDF._dataset_to_yaml(prefix, dataset_item, dest)

            # Extract each band from the NetCDF and write to individual GeoTIFF files
            COGNetCDF._dataset_to_cog(prefix,
                                      subdatasets,
                                      index + 1,
                                      dest,
                                      product_config)

            # Clean up XML files from GDAL
            # GDAL creates extra XML files which we don't want
            for xmlfile in dest.glob('*.xml'):
                xmlfile.unlink()

            generated_datasets[prefix] = dest

        return generated_datasets

    @staticmethod
    def _dataset_to_yaml(prefix, nc_dataset: xarray.DataArray, dest_dir):
        """
        Write the datasets to separate yaml files
        """
        yaml_fname = dest_dir / (prefix + '.yaml')
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
            logging.info("Writing dataset Yaml to %s", yaml_fname.name)

    @staticmethod
    def _dataset_to_cog(prefix, subdatasets, band_num, dest_dir, product_config):
        """
        Write the datasets to separate cog files
        """

        os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
        os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'

        bands_to_cog = product_config.cfg.get('bands_to_cog_convert')
        band_resampling_methods = product_config.cfg.get('band_resampling_methods')

        with tempfile.TemporaryDirectory() as tmpdir:
            for dts in subdatasets[:-1]:
                band_name = dts[0].split(':')[-1]

                # Only do specified bands if specified in config
                if bands_to_cog:
                    if band_name not in bands_to_cog:
                        continue

                # Resampling method of this band
                resampling_method = band_resampling_methods.get(band_name) if band_resampling_methods else None
                if not resampling_method:
                    resampling_method = product_config.cfg.get('default_resampling_method')
                # Default resampling method not specified in config
                if not resampling_method:
                    resampling_method = 'mode'

                out_fname = prefix + '_' + band_name + '.tif'
                try:

                    # copy to a tempfolder
                    temp_fname = pjoin(tmpdir, basename(out_fname))
                    to_cogtif = [
                        'gdal_translate',
                        '-of', 'GTIFF',
                        '-b', str(band_num),
                        dts[0],
                        temp_fname]
                    run_command(to_cogtif, tmpdir)

                    # Add Overviews
                    # gdaladdo - Builds or rebuilds overview images.
                    # 2, 4, 8,16, 32 are levels which is a list of integral overview levels to build.
                    add_ovr = [
                        'gdaladdo',
                        '-r', resampling_method,
                        '--config', 'GDAL_TIFF_OVR_BLOCKSIZE', '512',
                        temp_fname,
                        '2', '4', '8', '16', '32']
                    run_command(add_ovr, tmpdir)

                    # Convert to COG
                    cogtif = [
                        'gdal_translate',
                        '-co', 'TILED=YES',
                        '-co', 'COPY_SRC_OVERVIEWS=YES',
                        '-co', 'COMPRESS=DEFLATE',
                        '-co', 'ZLEVEL=9',
                        '--config', 'GDAL_TIFF_OVR_BLOCKSIZE', '512',
                        '-co', 'BLOCKXSIZE=512',
                        '-co', 'BLOCKYSIZE=512',
                        '-co', 'PREDICTOR=2',
                        '-co', 'PROFILE=GeoTIFF',
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

    @staticmethod
    def _dir_suffix(item, item_template, dir_template):
        """
        Given a prefix like 'LS_WATER_3577_9_-39_20180506102018000000' what is the directory structure?
        """

        aws_file_param_values = parse(item_template, item).__dict__['named']

        # parse time values
        date_param_values = {}
        time_value = aws_file_param_values.get('time')
        if time_value:
            year = time_value[0:4]
            month = time_value[4:6]
            day = time_value[6:8]
            date_param_values = {'year': year, 'month': month, 'day': day}

        # All available parameter values
        all_param_values = dict(aws_file_param_values, **date_param_values)

        # Fill aws_dir_suffix
        return dir_template.format(**all_param_values)

    def aws_dir_suffix(self, item):
        """
        Given a prefix like 'LS_WATER_3577_9_-39_20180506102018000000' what is the AWS directory structure?
        """

        return self._dir_suffix(item, self.cfg['dest_template'],
                                self.cfg['aws_dir_suffix']) if self.cfg.get('aws_dir_suffix') else ''

    def local_dir_suffix(self, item):
        """
        Given a prefix like 'LS_WATER_3577_9_-39_20180506102018000000' what is the local directory structure?
        """

        return self._dir_suffix(item, self.cfg['dest_template'],
                                self.cfg['local_dir_suffix']) if self.cfg.get('local_dir_suffix') else ''

    def get_unstacked_names(self, netcdf_file, year=None, month=None):
        """
        Return the dataset prefix names corresponding to each dataset within the given NetCDF file.
        """

        src_file_param_values = parse(self.cfg['src_template'], basename(netcdf_file)).__dict__['named']
        names = []
        if self.cfg['time_taken_from'] == 'notime':
            names.append(self.cfg['dest_template'].format(**src_file_param_values))
        elif self.cfg['time_taken_from'] == 'filename':
            date_param_values = {}
            time_value = src_file_param_values.get('time')
            if time_value:
                # Do you want to get rid of nano seconds in this case
                time_without_nanosec = time_value[0:14]
                year_ = time_value[0:4]
                month_ = time_value[4:6]
                day = time_value[6:8]
                date_param_values = {'time': time_without_nanosec, 'year': year_, 'month': month_, 'day': day}

            # All available parameter values
            all_param_values = dict(src_file_param_values, **date_param_values)

            names.append(self.cfg['dest_template'].format(**all_param_values))
        else:
            # if time_type is not flat we assume it is timed
            dts = Dataset(netcdf_file)
            dts_times = dts.variables['time']
            for index, dt in enumerate(dts_times):
                dt_ = datetime.fromtimestamp(dt)
                # With nanosecond -use '%Y%m%d%H%M%S%f'
                time_stamp = to_datetime(dt_).strftime('%Y%m%d%H%M%S')
                year_ = time_stamp[0:4]
                month_ = time_stamp[4:6]
                day = time_stamp[6:8]
                time_param_values = {'time': time_stamp, 'year': year_, 'month': month_, 'day': day}

                # All available parameter values
                all_param_values = dict(src_file_param_values, **time_param_values)

                if year:
                    if month:
                        if dt_.year == year and dt_.month == month:
                            names.append(self.cfg['dest_template'].format(**all_param_values))
                    elif dt_.year == year:
                        names.append(self.cfg['dest_template'].format(**all_param_values))
                else:
                    names.append(self.cfg['dest_template'].format(**all_param_values))
        return names


@click.command()
@click.option('--config', '-c', help='Config file')
@click.option('--output-dir', '-o', help='Output directory', required=True)
@click.option('--product', '-p', help='Product name', required=True)
@click.argument('filename', nargs=1, type=click.Path())
def convert_cog(config, output_dir, product, filename):
    """
    Convert a list of NetCDF files into Cloud Optimise GeoTIFF format

    Uses a configuration file to define the file naming schema.
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)-8s] %(message)s')

    if config:
        with open(config, 'r') as cfg_file:
            cfg = yaml.load(cfg_file)
    else:
        cfg = yaml.load(DEFAULT_CONFIG)

    output_dir = Path(output_dir)

    product_config = COGProductConfiguration(cfg['products'][product])

    generated_datasets = COGNetCDF.netcdf_to_cog(filename, dest_dir=working_dir, product_config=product_config)

    for generated_cog_dict in generated_datasets:
        # Submit to completed Queue
        for prefix, dataset_directory in generated_cog_dict.items():
            (output_dir / product_config.local_dir_suffix(prefix)).mkdir(parents=True, exist_ok=True)
            for child in dataset_directory.iterdir():
                child.rename(output_dir / product_config.local_dir_suffix(prefix) / child.name)


if __name__ == '__main__':
    convert_cog()

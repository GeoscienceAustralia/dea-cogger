import re
from pathlib import Path
from typing import Union

import structlog
import xarray
import yaml
from osgeo import gdal
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from dea_cog_converter.cogeo import cog_translate

# Note: DEFLATE compression while more efficient than LZW can cause compatibility issues
#       with some software packages
#       DEFLATE or LZW can be used for lossless compression, or
#       JPEG for lossy compression
DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}
DEFAULT_PROFILE = {'driver': 'GTiff',
                   'interleave': 'pixel',
                   'tiled': True,
                   'blockxsize': 512,  # 256 or 512 pixels
                   'blockysize': 512,  # 256 or 512 pixels
                   'compress': 'DEFLATE',
                   'copy_src_overviews': True,
                   'zlevel': 9}
LOG = structlog.get_logger()


class COGException(Exception):
    pass


class NetCDFCOGConverter:
    """
    Convert the input files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, no_overviews=None, default_resampling='average',
                 bands_rsp=None, prefix=None, predictor=2, **kwargs):
        # A list of keywords of bands which don't require resampling
        self.no_overviews = no_overviews if no_overviews is not None else []

        # A list of keywords of bands excluded in cog convert
        self.black_list = black_list

        # A list of keywords of bands to be converted
        self.white_list = white_list

        self.bands_rsp = bands_rsp if bands_rsp is not None else {}
        self.s3_prefix_path = prefix

        self.predictor = predictor

        self.default_resampling = default_resampling

    def __call__(self, input_fname, output_prefix):
        Path(output_prefix).parent.mkdir(parents=True, exist_ok=True)
        self.generate_cog_files(input_fname, output_prefix)

    def generate_cog_files(self, input_file, output_prefix):
        """
        Convert the datasets from the input file to COG format and save them in the 'dest_dir'

        Each dataset is put in a separate directory.

        The directory names will look like 'LS_WATER_3577_9_-39_20180506102018'
        """

        # Extract the #part=?? number if it exists in the filename, as used by ODC
        part_index = 0
        if '#' in input_file:
            input_file, part_no = input_file.split('#')
            _, part_index = part_no.split('=')
            part_index = int(part_index)

        if not input_file.endswith('.nc'):
            raise COGException("COG Converter only works with NetCDF datasets.")

        yaml_fname = output_prefix.with_suffix('.yaml')

        if yaml_fname.exists():
            raise COGException(f'Dataset Document {yaml_fname} already exists.')

        # Extract each band from the input file and write to individual GeoTIFF files
        self._netcdf_to_cogs(input_file, part_index, output_prefix)

        # Create a single yaml file for a sub-dataset (consolidated one for a band group)
        self._netcdf_to_yaml(input_file, part_index, output_prefix)

    def _netcdf_to_yaml(self, input_file: Union[str, Path], part_index, output_prefix):
        """
        Write the datasets to separate yaml files
        """

        yaml_fname = output_prefix.with_suffix('.yaml')

        dataset_array = xarray.open_dataset(input_file)
        if len(dataset_array.dataset) == 1:
            dataset_object = dataset_array.dataset.item().decode('utf-8')
        else:
            dataset_object = dataset_array.dataset.isel(time=part_index).item().decode('utf-8')

        dataset = yaml.load(dataset_object, Loader=Loader)
        if dataset is None:
            LOG.info(f'No YAML section {output_prefix}')
            return

        invalid_band = []
        # Update band urls
        for band_name, band_definition in dataset['image']['bands'].items():
            if self.black_list is not None:
                if re.search(self.black_list, band_name) is not None:
                    invalid_band.append(band_name)
                    continue

            # TODO WTF
            if self.white_list is not None:
                if re.search(self.white_list, band_name) is None:
                    invalid_band.append(band_name)
                    continue

            tif_path = f'{output_prefix.name}_{band_name}.tif'

            band_definition.pop('layer', None)
            band_definition['path'] = tif_path

        for band in invalid_band:
            dataset['image']['bands'].pop(band, None)

        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}

        with open(yaml_fname, 'w') as fp:
            yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
            LOG.info(f"Created yaml file, {yaml_fname}")

    def _netcdf_to_cogs(self, input_file, part_index, output_prefix):
        """
        Write the datasets to separate cog files
        """
        try:
            dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        except Exception as exp:
            LOG.exception(f"GDAL input file error {input_file}: \n{exp}")
            return

        if dataset is None:
            return

        subdatasets = dataset.GetSubDatasets()

        profile = DEFAULT_PROFILE.copy()
        profile['predictor'] = self.predictor

        for dts in subdatasets[:-1]:  # Skip the last dataset, since that is the metadata doc

            # Band Name is the last of the colon separate elements in GDAL
            band_name = dts[0].split(':')[-1]

            out_fname = output_prefix.parent / f'{output_prefix.name}_{band_name}.tif'

            # Check the done files might need a force option later
            if out_fname.exists():
                if self._check_tif(out_fname):
                    continue

            # Resampling method of this band
            resampling_method = self.bands_rsp.get(band_name, self.default_resampling)

            if band_name in self.no_overviews:
                resampling_method = None

            cog_translate(dts[0], str(out_fname),
                          profile,
                          indexes=[part_index + 1],
                          overview_resampling=resampling_method,
                          config=DEFAULT_GDAL_CONFIG)

    @staticmethod
    def _check_tif(fname):
        try:
            cog_tif = gdal.Open(str(fname), gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except Exception:
            LOG.exception(f"Exception opening {fname}")
            return False

        if t_stats > [0.] * 4:
            return True
        else:
            return False

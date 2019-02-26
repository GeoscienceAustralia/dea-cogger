"""rio_cogeo.cogeo: translate a file to a cloud optimized geotiff."""
import os
import re
from pathlib import Path
from typing import Union

import gdal
import numpy
import rasterio
import structlog
import xarray
import yaml
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.shutil import copy
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}
# Note: DEFLATE compression while more efficient than LZW can cause compatibility issues
#       with some software packages
#       DEFLATE or LZW can be used for lossless compression, or
#       JPEG for lossy compression
DEFAULT_PROFILE = {'driver': 'GTiff',
                   'interleave': 'pixel',
                   'tiled': True,
                   'blockxsize': 512,  # 256 or 512 pixels
                   'blockysize': 512,  # 256 or 512 pixels
                   'compress': 'DEFLATE',
                   'copy_src_overviews': True,
                   'zlevel': 9}
LOG = structlog.get_logger()

# GDAL Initialisation
os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'
gdal.UseExceptions()


class COGException(Exception):
    pass


class NetCDFCOGConverter:
    """
    Convert the input files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, no_overviews=None, default_resampling='average',
                 bands_rsp=None, name_template=None, prefix=None, predictor=2):
        # A list of keywords of bands which don't require resampling
        self.no_overviews = no_overviews if no_overviews is not None else []

        # A list of keywords of bands excluded in cog convert
        self.black_list = black_list

        # A list of keywords of bands to be converted
        self.white_list = white_list

        self.bands_rsp = bands_rsp if bands_rsp is not None else {}
        self.name_template = name_template
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

    def _check_tif(self, fname):
        try:
            cog_tif = gdal.Open(fname, gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except Exception:
            LOG.exception(f"Exception opening {fname}")
            return False

        if t_stats > [0.] * 4:
            return True
        else:
            return False


def cog_translate(
        src_path,
        dst_path,
        dst_kwargs,
        indexes=None,
        overview_level=5,
        overview_resampling=None,
        config=None,
):
    """
    Create Cloud Optimized Geotiff.

    Parameters
    ----------
    src_path : str or PathLike object
        A dataset path or URL. Will be opened in "r" mode.
    dst_path : str or Path-like object
        An output dataset path or or PathLike object.
        Will be opened in "w" mode.
    dst_kwargs: dict
        output dataset creation options.
    indexes : tuple, int, optional
        Raster band indexes to copy.
    overview_level : int, optional (default: 6)
        COGEO overview (decimation) level
    overview_resampling : str, [average, nearest, mode]
    config : dict
        Rasterio Env options.

    """
    config = config or {}

    nodata_mask = None
    src = gdal.Open(src_path, gdal.GA_ReadOnly)
    band = src.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    if band.DataType == gdal.GDT_Byte and band.GetNoDataValue() < 0:
        nodata_mask = 255

    with rasterio.Env(**config):
        with rasterio.open(src_path) as src:

            indexes = indexes if indexes else src.indexes
            meta = src.meta
            meta["count"] = len(indexes)
            meta.pop("alpha", None)

            meta.update(**dst_kwargs)
            meta.pop("compress", None)
            meta.pop("photometric", None)
            if nodata_mask is not None:
                meta['nodata'] = nodata
                meta['dtype'] = 'int16'
            meta['stats'] = True

            with MemoryFile() as memfile:
                with memfile.open(**meta) as mem:
                    wind = list(mem.block_windows(1))
                    for ij, w in wind:
                        matrix = src.read(window=w, indexes=indexes)
                        if nodata_mask is not None:
                            matrix = numpy.array(matrix, dtype='int16')
                            matrix[matrix == nodata_mask] = nodata

                        mem.write(matrix, window=w)

                    if overview_resampling is not None:
                        overviews = [2 ** j for j in range(1, overview_level + 1)]

                        mem.build_overviews(overviews, Resampling[overview_resampling])
                        mem.update_tags(
                            OVR_RESAMPLING_ALG=Resampling[overview_resampling].name.upper()
                        )

                    try:
                        copy(mem, dst_path, **dst_kwargs)
                        LOG.info(f"Created a cloud optimized GeoTIFF file, {dst_path}")
                    except Exception:
                        LOG.exception(f"Error while creating a cloud optimized GeoTIFF file, {dst_path}")
                        raise

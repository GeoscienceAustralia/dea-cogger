"""rio_cogeo.cogeo: translate a file to a cloud optimized geotiff."""
import gdal
import numpy
import os
import re
import rasterio
import sys
import xarray
import yaml

from os.path import basename, exists
from pathlib import Path
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.shutil import copy
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}


class COGConvert:
    """
    Convert the input files to COG style GeoTIFFs
    """

    def __init__(self, black_list=None, white_list=None, nonpym_list=None, default_rsp=None,
                 bands_rsp=None, name_template=None, prefix=None, predictor=None, stacked_name_template=None):
        # A list of keywords of bands which don't require resampling
        self.nonpym_list = nonpym_list

        # A list of keywords of bands excluded in cog convert
        self.black_list = black_list

        # A list of keywords of bands to be converted
        self.white_list = white_list

        self.bands_rsp = bands_rsp
        self.name_template = name_template
        self.s3_prefix_path = prefix
        self.stacked_name_template = stacked_name_template

        if predictor is None:
            self.predictor = 2
        else:
            self.predictor = predictor

        if default_rsp is None:
            self.default_rsp = 'average'
        else:
            self.default_rsp = default_rsp

    def __call__(self, in_filepath, dest_dir):
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        dst_prefix_path = Path(dest_dir) / basename(in_filepath).split('.')[0]
        self.generate_cog_files(in_filepath, str(dst_prefix_path))

    def generate_cog_files(self, input_file, dst_prefix_path):
        """
        Convert the datasets from the input file to COG format and save them in the 'dest_dir'

        Each dataset is put in a separate directory.

        The directory names will look like 'LS_WATER_3577_9_-39_20180506102018'
        """
        if input_file.endswith('.yaml'):
            print("Processing level 2 scenes")
            for fpath, subdirs, files in os.walk(Path(input_file).parent):
                for fname in files:
                    if fname.endswith('.tif'):
                        geotif_in_flpath = os.path.join(fpath, fname)
                        print(f"Cog Convert {basename(geotif_in_flpath)} file")
                        cogtif_out_flpath = Path(dst_prefix_path).parents[0] / fname

                        # Extract each band from the input file and write to individual GeoTIFF files
                        self._tif_to_cogtiff(geotif_in_flpath, str(cogtif_out_flpath))

            with open(input_file) as stream:
                dataset = yaml.load(stream, Loader=Loader)

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

            # Strip /product/cog_filename from the output path. Hence path.parents[1]
            yaml_out_fpath = Path(cogtif_out_flpath).parents[1] / basename(input_file)

            for band in invalid_band:
                dataset['image']['bands'].pop(band)

            dataset['format'] = {'name': 'GeoTIFF'}
            with open(yaml_out_fpath, 'w') as fp:
                yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
                print(f"Created yaml file, {yaml_out_fpath}")
            return

        try:
            dataset = gdal.Open(input_file, gdal.GA_ReadOnly)
        except Exception as exp:
            print(f"GDAL input file error {input_file}: \n{exp}", file=sys.stderr)
            return

        if dataset is None:
            return

        subdatasets = dataset.GetSubDatasets()

        # Extract each band from the input file and write to individual GeoTIFF files
        rastercount = self._dataset_to_cog(dst_prefix_path, subdatasets)

        dataset_array = xarray.open_dataset(input_file)

        # Create a single yaml file for a sub-dataset (consolidated one for a band group)
        self._dataset_to_yaml(dst_prefix_path, dataset_array, rastercount)
        # Clean up XML files from GDAL
        # GDAL creates extra XML files which we don't want

    def _tif_to_cogtiff(self, in_fpath, out_fpath):
        """ Convert the Geotiff to COG using cogeo cog_translate module
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
        os.environ['GDAL_DISABLE_READDIR_ON_OPEN'] = 'YES'
        os.environ['CPL_VSIL_CURL_ALLOWED_EXTENSIONS'] = '.tif'

        # Note: DEFLATE compression while more efficient than LZW can cause compatibility issues
        #       with some software packages
        #       DEFLATE or LZW can be used for lossless compression, or
        #       JPEG for lossy compression
        default_profile = {'driver': 'GTiff',
                           'interleave': 'pixel',
                           'tiled': True,
                           'blockxsize': 512,  # 256 or 512 pixels
                           'blockysize': 512,  # 256 or 512 pixels
                           'compress': 'DEFLATE',
                           'predictor': self.predictor,
                           'copy_src_overviews': True,
                           'zlevel': 9}
        cog_translate(in_fpath, out_fpath,
                      default_profile,
                      indexes=[1],
                      overview_resampling='average',
                      overview_level=6,
                      config=DEFAULT_GDAL_CONFIG)

    def _dataset_to_yaml(self, dst_prefix_path, dataset_array: xarray.Dataset, rastercount):
        """
        Write the datasets to separate yaml files
        """
        for i in range(rastercount):
            if rastercount == 1:
                yaml_fname = dst_prefix_path + '.yaml'
                dataset_object = (dataset_array.dataset.item()).decode('utf-8')
            else:
                yaml_fname = dst_prefix_path + '_' + str(i + 1) + '.yaml'
                dataset_object = (dataset_array.dataset.item(i)).decode('utf-8')

            if exists(yaml_fname):
                continue

            dataset = yaml.load(dataset_object, Loader=Loader)
            if dataset is None:
                print(f"No yaml section {dst_prefix_path}", file=sys.stderr)
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
                    tif_path = basename(dst_prefix_path + '_' + key + '.tif')
                else:
                    tif_path = basename(dst_prefix_path + '_' + key + '_' + str(i + 1) + '.tif')

                value['layer'] = str(i + 1)
                value['path'] = tif_path

            for band in invalid_band:
                dataset['image']['bands'].pop(band)

            dataset['format'] = {'name': 'GeoTIFF'}
            dataset['lineage'] = {'source_datasets': {}}
            with open(yaml_fname, 'w') as fp:
                yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
                print(f"Created yaml file, {yaml_fname}")

    def _dataset_to_cog(self, dst_prefix_path, subdatasets):
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

        rastercount = 1
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
                    out_fname = dst_prefix_path + '_' + band_name + '.tif'
                else:
                    out_fname = dst_prefix_path + '_' + band_name + '_' + str(i + 1) + '.tif'

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

                # Note: DEFLATE compression while more efficient than LZW can cause compatibility issues
                #       with some software packages
                #       DEFLATE or LZW can be used for lossless compression, or
                #       JPEG for lossy compression
                default_profile = {'driver': 'GTiff',
                                   'interleave': 'pixel',
                                   'tiled': True,
                                   'blockxsize': 512,  # 256 or 512 pixels
                                   'blockysize': 512,  # 256 or 512 pixels
                                   'compress': 'DEFLATE',
                                   'predictor': self.predictor,
                                   'copy_src_overviews': True,
                                   'zlevel': 9}
                cog_translate(dts[0], out_fname,
                              default_profile,
                              indexes=[i + 1],
                              overview_resampling=resampling_method,
                              config=DEFAULT_GDAL_CONFIG)

        return rastercount

    def _check_tif(self, fname):
        try:
            cog_tif = gdal.Open(fname, gdal.GA_ReadOnly)
            srcband = cog_tif.GetRasterBand(1)
            t_stats = srcband.GetStatistics(True, True)
        except Exception as exp:
            print(f"Exception: {exp}", file=sys.stderr)
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
                        print(f"Created a cloud optimized GeoTIFF file, {dst_path}")
                    except Exception as exp:
                        print(f"Error while creating a cloud optimized GeoTIFF file, {dst_path}\n\t{exp}",
                              file=sys.stderr)
                        raise

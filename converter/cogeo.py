"""rio_cogeo.cogeo: translate a file to a cloud optimized geotiff."""
from datetime import datetime
import gdal
import numpy
import os
import re
import rasterio
import sys
import xarray
import yaml

from os.path import join as pjoin, basename, exists, split
from pathlib import Path
from rasterio.io import MemoryFile
from rasterio.enums import Resampling
from rasterio.shutil import copy
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

DEFAULT_GDAL_CONFIG = {'NUM_THREADS': 1, 'GDAL_TIFF_OVR_BLOCKSIZE': 512}


class COGNetCDF:
    """
    Convert NetCDF files to COG style GeoTIFFs
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
        self.prefix = prefix
        self.stacked_name_template = stacked_name_template

        if predictor is None:
            self.predictor = 2
        else:
            self.predictor = predictor

        if default_rsp is None:
            self.default_rsp = 'average'
        else:
            self.default_rsp = default_rsp

    def __call__(self, input_fname, dest_dir):
        prefix_name = self._make_outprefix(input_fname, dest_dir)
        self.netcdf_to_cog(input_fname, prefix_name)

    def _make_s1_outprefix(self, input_fname, dest_dir):
        abs_fname = split(input_fname)[0]
        dirname = basename(abs_fname)
        coords = basename(split(abs_fname)[0])

        prefix_name = re.search(r"[-\w\d.]*(?=\.\w)", abs_fname).group(0)
        r = re.compile(r"(?<=_)[-\d.T]+")
        indices = r.findall(prefix_name)

        dest_dict = {"time": datetime.strptime(indices[1].replace('T', ''), '%Y%m%d%H%M%S'), "coord": coords}
        return Path(dest_dir).joinpath(self.name_template.format(**dest_dict)).joinpath(dirname), dirname

    def _make_outprefix(self, input_fname, dest_dir):
        abs_fname = basename(input_fname)
        prefix_name = re.search(r"[-\w\d.]*(?=\.\w)", abs_fname).group(0)
        r = re.compile(r"(?<=_)[-\d.]+")
        indices = r.findall(prefix_name)
        r = re.compile(r"(?<={)\w+")
        keys = sorted(set(r.findall(self.name_template)))

        if len(indices) > len(keys):
            indices = indices[-len(keys):]

        indices += [None] * (3 - len(indices))
        x_index, y_index, date_time = indices

        if indices == [None] * len(indices):
            out_dir, prefix_name = self._make_s1_outprefix(input_fname, dest_dir)
        else:
            dest_dict = {keys[1]: x_index, keys[2]: y_index}

            if date_time is not None:
                try:
                    dest_dict[keys[0]] = datetime.strptime(date_time, '%Y%m%d%H%M%S%f')
                except ValueError:
                    dest_dict[keys[0]] = datetime.strptime(date_time, '%Y')  # Stacked netCDF file
            else:
                self.name_template = '/'.join(self.name_template.split('/')[0:2])

            out_dir = Path(pjoin(dest_dir, self.name_template.format(**dest_dict))).parents[0]

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
        except Exception as exp:
            print(f"netcdf error {input_file}: \n{exp}", file=sys.stderr)
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
                print(f"No yaml section {prefix}", file=sys.stderr)
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

                print(f"{dts[0]}", file=sys.stdout)
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
                        copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)
                    except Exception as exp:
                        print(f"Error while cog conversion: {exp}", file=sys.stderr)
                        raise

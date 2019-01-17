"""rio_cogeo.cogeo: translate a file to a cloud optimized geotiff."""
import gdal
import numpy
import rasterio
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.shutil import copy


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
    src = None

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

                    copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)

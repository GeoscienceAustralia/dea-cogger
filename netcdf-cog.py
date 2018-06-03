from os.path import join as pjoin, basename, dirname, exists, splitext
import tempfile
from subprocess import check_call
import subprocess
import click
import os
import logging
from osgeo import gdal
import xarray
import yaml
from yaml import CLoader as Loader, CDumper as Dumper


def run_command(command, work_dir): 
    """ 
    A simple utility to execute a subprocess command. 
    """ 
    try:
        check_call(command, stderr=subprocess.STDOUT, cwd=work_dir)
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))


def check_dir(fname):
    file_name = fname.split('/')
    rel_path = pjoin(*file_name[-2:])
    file_wo_extension, extension = splitext(rel_path)
    return file_wo_extension


def getfilename(fname, outdir):
    """ To create a temporary filename to add overviews and convert to COG
        and create a file name just as source but with '.TIF' extension
    """
    file_path = check_dir(fname)
    out_fname = pjoin(outdir, file_path)
    if not exists(dirname(out_fname)):
        os.makedirs(dirname(out_fname))
    return out_fname, file_path


def get_bandname(filename):
    return (filename.split(':'))[-1]


def add_image_path(bands, fname, rc, count):
    for key, value in bands.items():
        value['layer'] = '1'
        if rc > 1:
            value['path'] = basename(fname) + '_' + str(count+1) + '_' + key + '.tif'
        else:
            value['path'] = basename(fname) + '_' + key + '.tif'
    return bands


def _write_dataset(fname, file_path, outdir, rastercount):
    dataset_array = xarray.open_dataset(fname)
    for count in range(rastercount):
        if rastercount > 1:
            y_fname = file_path + '_' + str(count+1) + '.yaml'
            dataset_object = (dataset_array.dataset.item(count)).decode('utf-8')
        else:
            y_fname = file_path + '.yaml'
            dataset_object = (dataset_array.dataset.item()).decode('utf-8')
        yaml_fname = pjoin(outdir, y_fname)
        dataset = yaml.load(dataset_object, Loader=Loader)
        bands = dataset['image']['bands']
        dataset['image']['bands'] = add_image_path(bands, file_path, rastercount, count)
        dataset['format'] = {'name': 'GeoTIFF'}
        dataset['lineage'] = {'source_datasets': {}}
        with open(yaml_fname, 'w') as fp:
            yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)
            logging.info("Writing dataset Yaml to %s", basename(yaml_fname))


def _write_cogtiff(out_f_name, outdir, subdatasets, rastercount):
    """ Convert the Geotiff to COG using gdal commands
        Blocksize is 512
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        for netcdf in subdatasets[:-1]:
            for count in range(1, rastercount + 1):
                band_name = get_bandname(netcdf[0])
                if rastercount > 1:
                    out_fname = out_f_name + '_' + str(count) + '_' + band_name + '.tif'
                else:
                    out_fname = out_f_name + '_' + band_name + '.tif'

                env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
                       'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
                subprocess.check_call(env, shell=True)

                # copy to a tempfolder
                temp_fname = pjoin(tmpdir, basename(out_fname))
                to_cogtif = [
                    'gdal_translate',
                    '-b',
                    str(count),
                    netcdf[0],
                    temp_fname]
                run_command(to_cogtif, tmpdir)

                # Add Overviews
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
                run_command(cogtif, outdir)


@click.command(help="\b Convert netcdf to Geotiff and then to Cloud Optimized Geotiff using gdal."
" Mandatory Requirement: GDAL version should be <=2.2")
@click.option('--path', '-p', required=True, help="Read the netcdfs from this folder",
              type=click.Path(exists=True, readable=True))
@click.option('--output', '-o', required=True, help="Write COG's into this folder",
              type=click.Path(exists=True, writable=True))
def main(path, output):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    netcdf_path = os.path.abspath(path)
    output_dir = os.path.abspath(output)
    count = 0
    rastercount = 0
    for path, subdirs, files in os.walk(netcdf_path):
        for fname in files:
            f_name = pjoin(path, fname)
            logging.info("Reading %s", basename(f_name))
            gtiff_fname, file_path = getfilename(f_name, output_dir)
            dataset = gdal.Open(f_name, gdal.GA_ReadOnly)
            subdatasets = dataset.GetSubDatasets()
            # ---To Check if NETCDF is stacked or unstacked --
            sds_open = gdal.Open(subdatasets[0][0])
            rastercount = sds_open.RasterCount
            dataset = None
            _write_dataset(f_name, file_path, output_dir, rastercount)
            _write_cogtiff(gtiff_fname, output_dir, subdatasets, rastercount)
            logging.info("Writing COG to %s", basename(gtiff_fname))

               
if __name__ == "__main__":
    main()

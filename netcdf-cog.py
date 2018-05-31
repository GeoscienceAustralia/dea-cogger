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
remove = ['rm', '*.aux.xml']

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
    file, extension = splitext(rel_path)
    yaml_file = file+'.yaml'
    return yaml_file, file


def getfilename(fname, outdir):
    """ To create a temporary filename to add overviews and convert to COG
        and create a file name just as source but with '.TIF' extension
    """
    yaml_fname, rel_path = check_dir(fname)
    out_fname = pjoin(outdir, rel_path)
    out_yfname = pjoin(outdir, yaml_fname)
    if not exists(dirname(out_yfname)):
        os.makedirs(dirname(out_yfname))
    return out_yfname, out_fname, rel_path


def get_bandname(filename):
    return (filename.split(':'))[-1]


def get_s3_url(fname, band_name):
    s3_bucket_name = 'dea-public-data'
    s3_obj_key = 'wofs-public'
    return 'http://{bucket_name}.s3.amazonaws.com/{obj_key}/'.format(bucket_name=s3_bucket_name, obj_key=s3_obj_key) + \
           fname + '_' + band_name + '.tif'


def add_image_path(bands, fname):
    for key, value in bands.items():
        value['layer'] = '1'
        value['path'] = get_s3_url(fname, key)
    return bands


def _write_dataset(fname, yaml_fname, rel_path):
    dataset_array = xarray.open_dataset(fname)
    dataset_object = (dataset_array.dataset.item()).decode('utf-8')
    dataset = yaml.load(dataset_object, Loader=Loader)
    bands = dataset['image']['bands']
    dataset['image']['bands'] = add_image_path(bands, rel_path)
    dataset['format'] = {'name': 'GeoTiff'}
    dataset['lineage'] = {'source_datasets': {}}
    with open(yaml_fname, 'w') as fp:
        yaml.dump(dataset, fp, default_flow_style=False, Dumper=Dumper)


def _write_cogtiff(fname, out_f_name, outdir):
    """ Convert the Geotiff to COG using gdal commands
        Blocksize is retained to 1024
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        dataset = gdal.Open(fname, gdal.GA_ReadOnly)
        subdatasets = dataset.GetSubDatasets()
        dataset = None
        for netcdf in subdatasets[:-1]:
            band_name = get_bandname(netcdf[0])
            out_fname = out_f_name + '_' + band_name + '.tif'
            env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
                   'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
            subprocess.check_call(env, shell=True)

            # copy to a tempfolder
            temp_fname = pjoin(tmpdir, basename(out_fname))
            to_cogtif = [
                         'gdal_translate',
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
                       '1024',
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
@click.option('--path', '-p', required=True, help="Read the netcdfs from this folder", type=click.Path(exists=True, readable=True))
@click.option('--output', '-o', required=True, help="Write COG's into this folder",
              type=click.Path(exists=True, writable=True))
def main(path, output):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    netcdf_path = os.path.abspath(path)
    output_dir = os.path.abspath(output)
    count = 0
    for path, subdirs, files in os.walk(netcdf_path):
        for fname in files:
            f_name = pjoin(path, fname)
            logging.info("Reading %s", basename(f_name))
            yaml_fname, gtiff_fname, rel_path = getfilename(f_name, output_dir)
            _write_dataset(f_name, yaml_fname, rel_path)
            logging.info("Writing dataset Yaml to %s", basename(yaml_fname))
            _write_cogtiff(f_name, gtiff_fname, output_dir)
            count = count+1
            logging.info("Writing COG to %s, %i", basename(gtiff_fname), count)    
               
if __name__ == "__main__":
    main()

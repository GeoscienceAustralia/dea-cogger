from os.path import join as pjoin, basename, dirname, exists
import tempfile
from subprocess import check_call
import subprocess
import click
import os
import logging


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
    return rel_path


def getfilename(fname, outdir):
    """ To create a temporary filename to add overviews and convert to COG
        and create a file name just as source but without '.TIF' extension
    """
    rel_path = check_dir(fname)
    out_fname = pjoin(outdir, rel_path)

    if not exists(dirname(out_fname)): 
        os.makedirs(dirname(out_fname)) 
    return out_fname


def _write_cogtiff(fname, out_fname, outdir):
    """ Convert the Geotiff to COG using gdal commands
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
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_fname = pjoin(tmpdir, basename(fname))
        
        env = ['GDAL_DISABLE_READDIR_ON_OPEN=YES',
               'CPL_VSIL_CURL_ALLOWED_EXTENSIONS=.tif']
        subprocess.check_call(env, shell=True)
        
        # copy to a tempfolder
        to_cogtif = [
                     'gdal_translate',
                     fname, 
                     temp_fname]
        run_command(to_cogtif, tmpdir)

        # Add Overviews
        # gdaladdo - Builds or rebuilds overview images.
        # 2, 4, 8,16,32 are levels which is a list of integral overview levels to build.
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


@click.command(help="\b Convert Geotiff to Cloud Optimized Geotiff using gdal."
                    " Mandatory Requirement: GDAL version should be <=2.2")
@click.option('--path', '-p', required=True, help="Read the Geotiffs from this folder",
              type=click.Path(exists=True, readable=True))
@click.option('--output', '-o', required=True, help="Write COG's into this folder",
              type=click.Path(exists=True, writable=True))
def main(path, output):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    gtiff_path = os.path.abspath(path)
    output_dir = os.path.abspath(output)
    count = 0
    for path, subdirs, files in os.walk(gtiff_path):
        for fname in files:
            print(fname)
            if fname.endswith('.tif'):
                f_name = os.path.join(path, fname)
                logging.info("Reading %s", basename(f_name))
                filename = getfilename(f_name, output_dir)
                _write_cogtiff(f_name, filename, output_dir)
                count = count+1
                logging.info("Writing COG to %s, %i", dirname(filename), count)

               
if __name__ == "__main__":
    main()

import threading
import queue
import click
import os
from os.path import join as pjoin, basename, dirname, exists
import tempfile
import subprocess
from subprocess import check_call

MAX_QUEUE_SIZE = 2


def run_command(command, work_dir):
    """
    Author: Harshu Rampur
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


def geotiff_to_cog(fname, out_fname, outdir):
    """ Author: Harshu Rampur
        Convert the Geotiff to COG using gdal commands
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
            '-of',
            'GTIFF',
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


def process_file(file, src, dest):
    src_name = os.path.join(src, file)
    out_name = getfilename(src_name, dest)
    geotiff_to_cog(src_name, out_name, dest)


def upload_to_s3(item, src, dest, job_file):
    # to be removed
    file = os.path.join('src', item)
    src_name = os.path.join(src, file)

    dest_name = os.path.join(dest, file)
    aws_copy = [
        'aws',
        's3',
        'cp',
        src_name,
        dest_name
    ]
    with tempfile.TemporaryDirectory() as tmpdir:
        run_command(aws_copy, tmpdir)

    # job control logs
    with open(job_file, 'a') as f:
        f.write(item + '\n')

    # Remove the file from the queue directory
    with tempfile.TemporaryDirectory() as tmpdir:
        run_command(['rm', src_name], tmpdir)


class Streamer(object):
    def __init__(self, src_dir, queue_dir, dest_url, job_dir, restart):
        self.src_dir = src_dir
        self.queue_dir = queue_dir

        # We are going to start with a empty queue_dir
        with tempfile.TemporaryDirectory() as tmpdir:
            run_command(['rm', '-rf', os.path.join(self.queue_dir, '*')], tmpdir)

        self.dest_url = dest_url
        self.job_dir = job_dir

        # if restart clear streamer_job_control.log
        job_file = os.path.join(self.job_dir, 'streamer_job_control.log')
        if restart and os.path.exists(job_file):
            with tempfile.TemporaryDirectory() as tmpdir:
                run_command(['rm', job_file], tmpdir)

        # Compute file list
        items_done = []
        if os.path.exists(job_file):
            with open(job_file) as f:
                items_done = f.read().splitlines()

        items_all = os.listdir(self.src_dir)
        self.items = [item for item in items_all if item not in items_done]
        self.items.sort(reverse=True)
        self.job_file = job_file

    def compute(self, processed_queue):
        while self.items:
            if not processed_queue.full():
                item = self.items.pop()
                process_file(item, self.src_dir, self.queue_dir)
                processed_queue.put(item)
        # Signal end of processing
        processed_queue.put(None)

    def upload(self, processed_queue):
        while True:
            item = processed_queue.get(block=True, timeout=None)
            if item is None:
                break
            upload_to_s3(item, self.queue_dir, self.dest_url, self.job_file)

    def run(self):
        processed_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
        producer = threading.Thread(target=self.compute, args=(processed_queue,))
        consumer = threading.Thread(target=self.upload, args=(processed_queue,))
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()


@click.command()
@click.option('--queue', '-q', required=True, help="Queue directory")
@click.option('--dest', '-d', required=True, help="Destination Url")
@click.option('--job', '-j', required=True, help="Job directory that store job tracking info")
@click.option('--restart', is_flag=True, help="Restarts the job ignoring prior work")
@click.argument('src', type=click.Path(exists=True))
def main(queue, dest, job, restart, src):
    restart_ = True if restart else False
    streamer = Streamer(src, queue, dest, job, restart_)
    streamer.run()


if __name__ == '__main__':
    main()

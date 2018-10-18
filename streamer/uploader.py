import logging
import os
import subprocess
import time
from datetime import timedelta, datetime
from pathlib import Path
from subprocess import check_output, run

import click

from streamer.streamer import run_command


@click.command()
@click.option('--output-dir', '-o', help='Output directory', required=True)
@click.option('--upload-destination', '-u', required=True, type=click.Path(),
              help="Upload destination, typically including the bucket as well as prefix.\n"
                   "eg. s3://dea-public-data/my-favourite-product")
@click.option('--retain-datasets', '-r', is_flag=True, help='Retain datasets rather than delete them after upload')
def upload(output_dir, upload_destination, retain_datasets):
    """
    Watch a Directory and upload new contents to Amazon S3
    """

    output_dir = Path(output_dir)

    ready_for_upload_dir = output_dir / 'TO_UPLOAD'
    failed_dir = output_dir / 'FAILED'
    complete_dir = output_dir / 'COMPLETE'

    max_wait_time_without_upload = timedelta(minutes=5)
    time_last_upload = None
    while True:
        datasets_ready = check_output(['ls', ready_for_upload_dir]).decode('utf-8').splitlines()
        for dataset in datasets_ready:
            src_path = f'{ready_for_upload_dir}/{dataset}'
            dest_file = f'{src_path}/upload-destination.txt'
            if os.path.exists(dest_file):
                with open(dest_file) as f:
                    dest_path = f.read().splitlines()[0]

                dest_path = f'{upload_destination}/{dest_path}'
                aws_copy = [
                    'aws',
                    's3',
                    'sync',
                    src_path,
                    dest_path,
                    '--exclude',
                    dest_file
                ]
                try:
                    print(dest_path)
                    run_command(aws_copy)
                except Exception as e:
                    logging.error("AWS upload error %s", dest_path)
                    logging.exception("Exception", e)
                    try:
                        run_command(['mv', '-f', '--', src_path, failed_dir])
                    except Exception as e:
                        logging.error("Failure moving dataset %s to FAILED dir", src_path)
                        logging.exception("Exception", e)
                else:
                    if retain_datasets:
                        try:
                            run_command(['mv', '-f', '--', src_path, complete_dir])
                        except Exception as e:
                            logging.error("Failure moving dataset %s to COMPLETE dir", src_path)
                            logging.exception("Exception", e)
                    else:
                        try:
                            run('rm -fR -- ' + src_path, stderr=subprocess.STDOUT, check=True, shell=True)
                        except Exception as e:
                            logging.error("Failure in queue: removing dataset %s", src_path)
                            logging.exception("Exception", e)
            time_last_upload = datetime.now()
        time.sleep(1)
        if time_last_upload:
            elapsed_time = datetime.now() - time_last_upload
            if elapsed_time > max_wait_time_without_upload:
                break
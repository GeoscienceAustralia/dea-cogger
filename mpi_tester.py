import os
from concurrent.futures import as_completed
from subprocess import run, PIPE

from mpi4py import MPI
from mpi4py.futures import MPIPoolExecutor
import time

import logging
import os
import re
import subprocess
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
from os.path import join as pjoin, basename
from pathlib import Path
from subprocess import check_output, run

import click
import gdal
import xarray
import yaml
from datacube import Datacube
from datacube.model import Range
from netCDF4 import Dataset
from pandas import to_datetime
from tqdm import tqdm
from yaml import CSafeLoader as Loader, CSafeDumper as Dumper

from parse import *
from parse import compile

comm = MPI.COMM_WORLD
rank = comm.Get_rank()


def find_some_proc_info(num):
    time.sleep(1)
    return num, rank, run('hostname', stdout=PIPE, check=True, encoding='utf8').stdout.strip(), os.environ['PATH']


if __name__ == '__main__':

    print(f'Universe size: {comm.Get_size()}')

    with MPIPoolExecutor() as executor:
        print(f'Universe size: {comm.Get_size()}')
        futures = [executor.submit(find_some_proc_info, num) for num in range(32)]
        print(f'Universe size: {comm.Get_size()}')

        for fut in as_completed(futures):
            print(fut.result())

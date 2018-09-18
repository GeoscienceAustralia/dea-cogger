#!/usr/bin/env python
"""
This module has functions to check that all the netcdf files for a given product
have been turned into Cloud Optimised GeoTIFFs (cogs) and uploaded to the aws s3 bucket.
a list of files that have not been uploaded to s3  -However because of changing
file name conventions, some files in the list may have been uploaded with a slightly
different name.  This is especially true for fractional-cover products fc_ls8 and fc_ls5.
usage:
    from check_sync import list_files_not_in_s3_product
    #specify the product and output directory
    missing_files = list_files_not_in_s3_product('WOFLs', '/g/data/r78/vmn547/check_sync')
"""
import os
import boto3
from streamer import *
import time
from pathlib import Path

#describes the products. more can be added
products = {
    'WOFLs':{'prefix':'WOfS/WOFLs/v2.1.0/combined',
             'suffixes':['_water.tif'],
             'nci_dir':'/g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf'},
    'wofs_summary':{'prefix':'WOfS/summary',
                    'suffixes':['_frequency.tif', '_count_wet.tif','_count_clear.tif'],
                    'nci_dir':'/g/data/fk4/datacube/002/WOfS/WOfS_Stats_25_2_1/netcdf'},
    'fc_ls8':{'prefix':'fractional-cover/fc/v2.2.0/ls8',
              'suffixes':['_UE.tif', '_PV.tif','_NPV.tif','_BS.tif'],
              'nci_dir':'/g/data/fk4/datacube/002/FC/LS8_OLI_FC'},
    'fc_ls5':{'prefix':'fractional-cover/fc/v2.2.0/ls5',
              'suffixes':['_UE.tif', '_PV.tif','_NPV.tif','_BS.tif'],
              'nci_dir':'/g/data/fk4/datacube/002/FC/LS5_TM_FC'}
}

def get_allthe_s3_files(strPrefix='WOfS',bucket='dea-public-data', suffix='_water.tif'):
    """returns 2 lists of object names matching the the specified bucket, prefix and suffix
        :param strPrefix: the prefix in s3 bucket
        :param bucket: the s3 bucket
        :param suffix: the file endings you're looking for
        :return:
                keys = list of the full names of matching objects in s3
                keys_short = list of the file name part of matching objects in s3
        """
    conn = boto3.client('s3')
    keys = []
    keys_short =[]
    kwargs = {'Bucket': bucket}
    while True:
        resp = conn.list_objects_v2(**kwargs, Prefix=strPrefix)
        for obj in resp['Contents']:
            key = obj['Key']
            if suffix in key:
                keys.append(key)
                shortname = str.split(key, '/')[-1]
                keys_short.append(shortname)
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    suffix = suffix.replace('.', '_')

    return keys, keys_short


def get_unstacked_names(netcdf_file, year=None, month=None):
    """
    Return the dataset names corresponding to each dataset within the given NetCDF file.
    """
    try:
        dts = Dataset(netcdf_file)
    except:
        return [netcdf_file]

    file_id = os.path.splitext(basename(netcdf_file))[0]
    prefix = "_".join(file_id.split('_')[0:-2])
    stack_info = file_id.split('_')[-1]

    dts_times = dts.variables['time']
    names = []
    for index, dt in enumerate(dts_times):
        dt_ = datetime.fromtimestamp(dt)
        time_stamp = to_datetime(dt_).strftime('%Y%m%d%H%M%S%f')
        if year:
            if month:
                if dt_.year == year and dt_.month == month:
                    names.append('{}_{}'.format(prefix, time_stamp))
            elif dt_.year == year:
                names.append('{}_{}'.format(prefix, time_stamp))
        else:
            names.append('{}_{}'.format(prefix, time_stamp))
    return names


def remove_ms_and_version(fname):
    """
        remove the mseconds and version number from the filename
    :param fname:
    :return: new_name
    """
    if 'v' in fname:
        new_name_parts = fname.split('_')
        if len(new_name_parts[-2])>14:
            new_name_parts[-2]= new_name_parts[-2][:-6]
        new_name = '_'.join(new_name_parts[:-1])
    else:
        new_name_parts = fname.split('_')
        if len(new_name_parts[-1]) > 14:
            new_name_parts[-1] = new_name_parts[-1][:-6]
        new_name = '_'.join(new_name_parts)
    return new_name


def list_files_not_in_s3(nci_path, prefix, suffix, output_dir):
    """

    :param nci_path: the directory on the nci where the products netcdf files are stored
    :param prefix:
    :param suffix:
    :param output_dir:
    :return:
            a list of files that have not been uploaded to s3 (However because of changing
            file name conventions, some files in the list may have been uploaded with a slightly
            different name.  This is especially true for fractional cover.
    """
    missing_s3_files_tot = []
    netcdf_path = os.path.abspath(nci_path)

    p = Path(netcdf_path)

    paths = [x for x in p.iterdir() if x.is_dir()]
    paths.sort()
    if len(paths)<1:
        paths =[netcdf_path]
    orig_prefix =prefix
    for nc_path in paths:
        missing_s3_files = []
        if len(paths)>1:
            tiles = nc_path.stem.split('_')
            prefix = f'{orig_prefix}/x_{tiles[0]}/y_{tiles[1]}/'
        print(prefix)
        keys, files_in_s3 = get_allthe_s3_files(strPrefix=prefix, bucket='dea-public-data', suffix=suffix)
        for path, subdirs, files in os.walk(nc_path):
            for nci_fname in files:
                if nci_fname.endswith('.nc'):
                    if nci_fname[:-3]+suffix in files_in_s3:
                        found =True
                    else:
                        try:
                            for fname in get_unstacked_names(os.path.join(path, nci_fname)):
                                if fname + suffix in files_in_s3:
                                    found = True
                                else:
                                    fname =remove_ms_and_version(fname)
                                    if fname + suffix in files_in_s3:
                                        found = True
                                    else:

                                        missing_s3_files.append(os.path.join(path, nci_fname))
                        except:
                            missing_s3_files.append(os.path.join(path, nci_fname))
        if len(missing_s3_files)>0:
            missing_s3_files.sort()
            missing_s3_files_tot.append(missing_s3_files)
            output_fname = os.path.join(output_dir,suffix + '_files_missing in_s3.txt')
            with open(output_fname, 'a') as f:
                for file in missing_s3_files:
                    f.write("%s\n" % file)

    return missing_s3_files_tot


def list_files_not_in_s3_product(product, output_dir):
    '''
    checks for a given product if all associated cogs have been uploaded to aws s3
    :param product: a product from products. 'wofs_summary', 'WOFLs' or 'fractional-cover'
    :param output_dir: the directory in which to write the file list all the missing cogs
    :return:
        a list of files that have not been uploaded to s3 (However because of changing
        file name conventions, some files in the list may have been uploaded with a slightly
        different name.  This is especially true for fractional cover.
    '''
    for suffix in products[product]['suffixes']:
        files = list_files_not_in_s3(products[product]['nci_dir'], products[product]['prefix'], suffix, output_dir)
    return files


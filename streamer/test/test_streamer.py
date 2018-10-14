from subprocess import run, check_call
import os

output_dir = '/g/data/u46/users/aj9439/aws/tmp'


def test_wofs_annual_summary():
    if os.path.exists(f'{output_dir}/TO_UPLOAD/WOFS_3577_9_-49_2017_summary'):
        run(['rm', '-rf', f'{output_dir}/TO_UPLOAD/WOFS_3577_9_-49_2017_summary'])
    check_call(
        ['python', '/g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py',
         'convert-cog', '--num-procs', '1', '--output-dir',
         output_dir, '--product', 'wofs_annual_summary',
         '/g/data/fk4/datacube/002/WOfS/WOfS_Stats_Ann_25_2_1/netcdf/WOFS_3577_9_-49_2017_summary.nc']
    )
    assert os.path.exists(f'{output_dir}/TO_UPLOAD/WOFS_3577_9_-49_2017_summary/upload-destination.txt')
    with open(f'{output_dir}/TO_UPLOAD/WOFS_3577_9_-49_2017_summary/upload-destination.txt', 'r') as dest:
        assert dest.readline() == 'x_9/y_-49/2017'


def test_wofs_albers():
    if os.path.exists(f'{output_dir}/TO_UPLOAD/LS_WATER_3577_23_-13_20180820235521'):
        run(['rm', '-rf', f'{output_dir}/TO_UPLOAD/LS_WATER_3577_23_-13_20180820235521'])
    check_call(
        ['python', '/g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py',
         'convert-cog', '--num-procs', '1', '--output-dir',
         output_dir, '--product', 'wofs_albers',
         '/g/data/fk4/datacube/002/WOfS/WOfS_25_2_1/netcdf/23_-13/'
         'LS_WATER_3577_23_-13_20180820235521000000_v1535940854.nc']
    )
    assert os.path.exists(f'{output_dir}/TO_UPLOAD/LS_WATER_3577_23_-13_20180820235521/upload-destination.txt')
    with open(f'{output_dir}/TO_UPLOAD/LS_WATER_3577_23_-13_20180820235521/upload-destination.txt', 'r') as dest:
        assert dest.readline() == 'x_23/y_-13/2018/08/20'


def test_filtered_summary():
    if os.path.exists(f'{output_dir}/TO_UPLOAD/wofs_filtered_summary_9_-49'):
        run(['rm', '-rf', f'{output_dir}/TO_UPLOAD/wofs_filtered_summary_9_-49'])
    check_call(
        ['python', '/g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py',
         'convert-cog', '--num-procs', '1', '--output-dir',
         output_dir, '--product', 'wofs_filtered_summary',
         '/g/data2/fk4/datacube/002/WOfS/WOfS_Filt_Stats_25_2_1/netcdf/'
         'wofs_filtered_summary_9_-49.nc']
    )
    assert os.path.exists(f'{output_dir}/TO_UPLOAD/wofs_filtered_summary_9_-49/upload-destination.txt')
    with open(f'{output_dir}/TO_UPLOAD/wofs_filtered_summary_9_-49/upload-destination.txt', 'r') as dest:
        assert dest.readline() == 'x_9/y_-49'


def test_fs5_fc_albers():
    dts = ['LS5_TM_FC_3577_7_-44_20100117110700', 'LS5_TM_FC_3577_7_-44_20100407100707',
           'LS5_TM_FC_3577_7_-44_20100202110704', 'LS5_TM_FC_3577_7_-44_20101203110600',
           'LS5_TM_FC_3577_7_-44_20100209111317', 'LS5_TM_FC_3577_7_-44_20100218110707',
           'LS5_TM_FC_3577_7_-44_20101023111216']
    for dt in dts:
        if os.path.exists(f'{output_dir}/TO_UPLOAD/{dt}'):
            run(['rm', '-rf', f'{output_dir}/TO_UPLOAD/{dt}'])
    check_call(
        ['python', '/g/data/u46/users/aj9439/PycharmProjects/COG-Conversion/streamer/streamer.py',
         'convert-cog', '--num-procs', '1', '--output-dir',
         output_dir, '--product', 'ls5_fc_albers',
         '/g/data/fk4/datacube/002/FC/LS5_TM_FC/'
         '7_-44/LS5_TM_FC_3577_7_-44_2010_v20171128015024.nc']
    )
    assert os.path.exists(f'{output_dir}/TO_UPLOAD/LS5_TM_FC_3577_7_-44_20101023111216/upload-destination.txt')
    with open(f'{output_dir}/TO_UPLOAD/LS5_TM_FC_3577_7_-44_20101023111216/upload-destination.txt', 'r') as dest:
        assert dest.readline() == 'x_7/y_-44/2010/10/23'
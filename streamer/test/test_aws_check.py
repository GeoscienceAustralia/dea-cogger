from streamer.aws_check import get_indexed_info, get_prefixes, subset_of_s3_keys, DEFAULT_CONFIG
from click.testing import CliRunner
import uuid
from os.path import exists
import yaml

cfg = yaml.load(DEFAULT_CONFIG)


def test_get_indexed_info():
    product = 'wofs_filtered_summary'
    dts = list(get_indexed_info(product))
    assert isinstance(dts[0][0], uuid.UUID)
    assert exists(dts[0][1])


def test_get_prefixes():
    product = 'wofs_filtered_summary'
    dts = list(get_indexed_info(product))
    prefixes = get_prefixes(dts[0][0], dts[0][1], cfg['products'][product])
    assert prefixes[0] == 'wofs_filtered_summary_9_-49'


def test_subset_of_s3_keys():
    product = 'wofs_filtered_summary'
    dts = list(get_indexed_info(product))
    prefixes = get_prefixes(dts[0][0], dts[0][1], cfg['products'][product])
    s3_keys = {'wofs_filtered_summary_9_-49_confidence.tif',
               'wofs_filtered_summary_9_-49_wofs_filtered_summary.tif',
               'wofs_filtered_summary_9_-49.yaml'}
    assert subset_of_s3_keys(s3_keys, prefixes[0], product)


if __name__ == '__main__':
    test_get_indexed_info()
    test_get_prefixes()
    test_subset_of_s3_keys()

from streamer import get_indexed_info, get_prefixes, compare_nci_with_aws
from streamer import DEFAULT_CONFIG
from click.testing import CliRunner
import uuid
from os.path import exists
import yaml
from datetime import datetime

cfg = yaml.load(DEFAULT_CONFIG)


def test_get_indexed_info():
    product = 'wofs_filtered_summary'
    dts = list(get_indexed_info(product))
    assert isinstance(dts[0][0], uuid.UUID)
    assert exists(dts[0][1])
    assert isinstance(dts[0][2], datetime)


def test_get_prefixes():
    product = 'wofs_filtered_summary'
    dts = list(get_indexed_info(product))
    prefixes = get_prefixes(dts[0][1], dts[0][2], cfg['products'][product])
    assert prefixes[0] == 'wofs_filtered_summary_9_-49'


def test_compare_nci_with_aws():
    product = 'wofs_albers'
    bucket = 'dea-public-data-dev'
    cfg = yaml.load(DEFAULT_CONFIG)
    result = compare_nci_with_aws(cfg, product, 2018, 7, bucket)
    for item in result['aws_but_not_nci']:
        print(f'excess-aws-file:{item}')
    for item in result['nci_but_not_aws']:
        print(f'missing-in-aws:{item}')


if __name__ == '__main__':
    test_get_indexed_info()
    test_get_prefixes()
    test_compare_nci_with_aws()


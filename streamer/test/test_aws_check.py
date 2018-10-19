from streamer.aws_check import get_indexed_info, get_prefixes, DEFAULT_CONFIG
from streamer.streamer import COGProductConfiguration
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
    product_config = COGProductConfiguration(cfg['products'][product])
    dts = list(get_indexed_info(product))
    prefixes = get_prefixes(dts[0][0], dts[0][1], product_config.cfg)
    assert prefixes[0] == 'wofs_filtered_summary_9_-49'


if __name__ == '__main__':
    test_get_indexed_info()
    test_get_prefixes()

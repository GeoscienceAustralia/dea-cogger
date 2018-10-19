from streamer.aws_check import get_indexed_info, get_prefixes, subset_of_s3_keys, _check_nci_to_s3, cli, DEFAULT_CONFIG
from streamer.streamer import COGProductConfiguration
from click.testing import CliRunner
import uuid
from os.path import exists, join
import yaml
import tempfile

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
    prefixes = get_prefixes(dts[0][0], dts[0][1], product_config)
    assert prefixes[0] == 'wofs_filtered_summary_9_-49'


def test_subset_of_s3_keys():
    product = 'wofs_filtered_summary'
    product_config = COGProductConfiguration(cfg['products'][product])
    dts = list(get_indexed_info(product))
    prefixes = get_prefixes(dts[0][0], dts[0][1], product_config)
    s3_keys = {'wofs_filtered_summary_9_-49_confidence.tif',
               'wofs_filtered_summary_9_-49_wofs_filtered_summary.tif',
               'wofs_filtered_summary_9_-49.yaml'}
    assert subset_of_s3_keys(s3_keys, prefixes[0], product)


def test_check_nci_to_s3():
    product = 'wofs_filtered_summary'
    bucket = 'dea-public-data-dev'
    output_file = 'check_output.txt'
    with tempfile.TemporaryDirectory() as tmpdir:
        _check_nci_to_s3(None, product, None, None, bucket, join(tmpdir, output_file))
        with open(join(tmpdir, output_file), 'r') as f:
            print(f.readlines())
    # runner = CliRunner()
    # with runner.isolated_filesystem():
    #     result = runner.invoke(cli, ['-p', product, '-b', bucket, output_file])
    #     assert result.exit_code == 0


if __name__ == '__main__':
    test_get_indexed_info()
    test_get_prefixes()
    test_subset_of_s3_keys()
    test_check_nci_to_s3()


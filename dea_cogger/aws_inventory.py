from types import SimpleNamespace
from io import BytesIO
from gzip import GzipFile
import csv
import json

from .aws_s3_client import make_s3_client, s3_fetch, s3_ls_dir


def _find_latest_manifest(prefix, s3):
    manifest_dirs = sorted(s3_ls_dir(prefix, s3=s3), reverse=True)

    for d in manifest_dirs:
        if d.endswith('/'):
            leaf = d.split('/')[-2]
            if leaf.endswith('Z'):
                return d + 'manifest.json'


def list_inventory(manifest, s3=None, **kw):
    """
    Returns a generator of S3 inventory records

    :param manifest: s3 url to manifest.json OR a dir in which the newest manifest.json is used.
    """
    s3 = s3 or make_s3_client(**kw)

    if manifest.endswith('/'):
        manifest = _find_latest_manifest(manifest, s3)

    info = s3_fetch(manifest, s3=s3)
    info = json.loads(info)

    must_have_keys = {'fileFormat', 'fileSchema', 'files', 'destinationBucket'}
    missing_keys = must_have_keys - set(info)
    if missing_keys:
        raise ValueError("Manifest file haven't parsed correctly")

    file_format = info['fileFormat']
    if file_format.upper() != 'CSV':
        raise ValueError('Data is not in CSV format')

    prefix = 's3://' + info['destinationBucket'].split(':')[-1] + '/'
    schema = tuple(info['fileSchema'].split(', '))
    data_urls = [prefix + f['key'] for f in info['files']]

    for u in data_urls:
        bb = s3_fetch(u, s3=s3)
        gz = GzipFile(fileobj=BytesIO(bb), mode='r')
        csv_rdr = csv.reader(line.decode('utf8') for line in gz)

        for rec in csv_rdr:
            rec = SimpleNamespace(**{k: v for k, v in zip(schema, rec)})
            yield rec

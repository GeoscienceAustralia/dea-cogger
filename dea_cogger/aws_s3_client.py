import boto3
import logging
from urllib.parse import urlparse

log = logging.getLogger(__name__)


def _botocore_default_region(session=None):
    if session is None:
        session = boto3.session.get_available_regions('s3')
    return session.region_name


def _auto_find_region(session=None):
    region_name = _botocore_default_region(session)

    if region_name is None:
        raise ValueError('AWS region name is not supplied and default region can not be found')

    return region_name


def make_s3_client(region_name=None,
                   session=None,
                   profile=None,
                   use_ssl=True):
    if session is None:
        if profile is None:
            session = boto3.session.Session()
        else:
            session = boto3.session.Session(profile_name=profile)

    if region_name is None:
        region_name = _auto_find_region(session)

    protocol = 'https' if use_ssl else 'http'

    s3 = session.client('s3',
                        endpoint_url='{}://s3.{}.amazonaws.com'.format(protocol, region_name))
    return s3


def _s3_url_parse(url):
    uu = urlparse(url)
    return uu.netloc, uu.path.lstrip('/')


def s3_ls_dir(uri, s3=None, aws_profile=None):
    bucket, prefix = _s3_url_parse(uri)

    if len(prefix) > 0 and not prefix.endswith('/'):
        prefix += '/'

    if not s3:
        s3 = make_s3_client(profile=aws_profile)

    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket,
                                   Prefix=prefix,
                                   Delimiter='/'):
        sub_dirs = page.get('CommonPrefixes', [])
        files = page.get('Contents', [])

        for p in sub_dirs:
            yield 's3://{bucket}/{path}'.format(bucket=bucket, path=p['Prefix'])

        for o in files:
            yield 's3://{bucket}/{path}'.format(bucket=bucket, path=o['Key'])


def s3_fetch(url, s3=None, aws_profile=None, **kwargs):
    if not s3:
        s3 = make_s3_client(profile=aws_profile)

    bucket, key = _s3_url_parse(url)
    oo = s3.get_object(Bucket=bucket, Key=key, **kwargs)
    return oo['Body'].read()

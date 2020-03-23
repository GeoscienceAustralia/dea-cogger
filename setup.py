from setuptools import setup, find_packages

setup(
    name="dea_cogger",
    version="0.1",
    packages=find_packages(),
    url='https://github.com/GeoscienceAustralia/digitalearthau',
    install_requires=[
        'click>=5.0',
        'datacube',
        'python-dateutil',
        'boto3',
        'requests_aws4auth',
        'pyyaml',
        'boltons',
        'python-dateutil',
        'structlog',
        'colorama',  # Needed for structlog's CLI output.
        'mpi4py',
        'gdal',
    ],
    package_data={
        '': ['*.yaml', '*/*.yaml'],
    },
    include_package_data=True,
    setup_requires=['wheel'],
    entry_points={
        'console_scripts': [
            'dea-cogger = dea_cogger.cog_conv_app:cli',
        ]
    },
)

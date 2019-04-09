from setuptools import setup, find_packages

setup(
    name='dea-cog-dea_cog_converter',
    version='0.2',
    license='Apache License 2.0',
    packages=['dea_cog_converter'],

    author='Geoscience Australia',
    author_email='',
    maintainer='Geoscience Australia',
    maintainer_email='',

    description='A tool for bulk conversion of NetCDF format data into Cloud Optimised Geotiff.\n\nIt is also able to '
                'compare an AWS S3 bucket with an ODC database to find datasets to be converted.',
    python_requires='>=3.5',
    install_requires=[
        'click',
        'pyyaml',
        'gdal',
        'numpy',
        'xarray',
        'structlog',
        'datacube',
        'tqdm',
        'mpi4py',
        'python-dateutils',
        'rasterio>=1.0.22',
    ],
    entry_points={
        'console_scripts': [
            'dea-cogger = dea_cog_converter.cli:cli',
        ]
    },
    tests_require=['pytest'],
)




# Digital Earth Australia NetCDF to COG conversion
NetCDF to COG conversion from NCI file system

- Convert NetCDF files that are on NCI /g/data/ file system and save them to the output path provided
- Use `dea-cogger` to convert to COG:


## Example conversion file list

```csv
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/6_-35/LS7_ETM_FC_3577_6_-35_2001_v20171128022741.nc#part=31,x_6/y_-35/2001/06/21/LS7_ETM_FC_3577_6_-35_20010621002821
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/-12_-28/LS7_ETM_FC_3577_-12_-28_2002_v20171128022751.nc#part=43,x_-12/y_-28/2002/07/14/LS7_ETM_FC_3577_-12_-28_20020714013928
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/-5_-21/LS7_ETM_FC_3577_-5_-21_2001_v20171128022741.nc#part=3,x_-5/y_-21/2001/01/11/LS7_ETM_FC_3577_-5_-21_20010111012150
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/17_-29/LS7_ETM_FC_3577_17_-29_2000_v20171128022730.nc#part=3,x_17/y_-29/2000/01/31/LS7_ETM_FC_3577_17_-29_20000131235302
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/-14_-26/LS7_ETM_FC_3577_-14_-26_2002_v20171128022751.nc#part=28,x_-14/y_-26/2002/06/17/LS7_ETM_FC_3577_-14_-26_20020617015721
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/1_-16/LS7_ETM_FC_3577_1_-16_1999_v20171128022657.nc#part=19,x_1/y_-16/1999/10/16/LS7_ETM_FC_3577_1_-16_19991016010411
/g/data/fk4/datacube/002/FC/LS7_ETM_FC/-6_-29/LS7_ETM_FC_3577_-6_-29_2002_v20171128022751.nc#part=12,x_-6/y_-29/2002/04/13/LS7_ETM_FC_3577_-6_-29_20020413011535
```

#### Usage

```
>  [~]$ dea-cogger --help
Usage: cog_conv_app.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  convert             Convert a single/list of files into COG format
  generate-work-list  Generate task list for COG conversion
  mpi-convert         Bulk COG conversion using MPI
  qsub_cog            Kick off five stage COG Conversion PBS job
  save-s3-inventory   Save S3 inventory list in a pickle file
  verify              Verify GeoTIFFs are Cloud Optimised GeoTIFF

```

##### Example of a Yaml file:
```yaml
    products:
        product_name:
            prefix: WOfS/WOFLs/v2.1.5/combined
            name_template: x_{x}/y_{y}/{time:%Y}/{time:%m}/{time:%d}/LS_WATER_3577_{x}_{y}_{time:%Y%m%d%H%M%S%f}
            predictor: 2
            no_overviews: ["source", "observed"]
            default_resampling: average
            white_list: None
            black_list: None
```
where:
      product_name:              A unique user defined string (required)
      prefix:                    Define the cogs folder structure and name (required)
      name_template:             Define how to decipher the input file names (required)
      default_resampling:        Define the resampling method of pyramid view (default: average)
      predictor:                 Define the predictor in COG convert (default: 2)
      no_overviews:              A list of keywords of bands which don't require resampling (optional)
      white_list:                A list of keywords of bands to be converted (optional)
      black_list:                A list of keywords of bands excluded in cog convert (optional)

Note: `no_overviews` contains the names of the band for which you don't want to generate overviews.
      This element cannot be used with other products as this because it will match as *source*'.
      For most products, this element is not needed. So far, only fractional cover percentile uses this.
```
##### What to set for predictor and resampling:

Predictor
<int> (1=default, 2=Horizontal differencing, 3 =floating point prediction)
**Horizontal differencing** is particularly useful for 16-bit data when the high-order and low-order bytes are
changing at different frequencies.Predictor=2 option should improve compression on any files greater than 8 bits/resel.
The **floating point** predictor PREDICTOR=3 results in significantly better compression ratios for floating point data.
There doesn't seem to be a performance penalty either for writing data with the floating point predictor, so it's a
pretty safe bet for any Float32 data.

Raster Resampling
*default_resampling* <resampling method> (average, nearest, mode)
**nearest**: Nearest neighbor has a tendency to leave artifacts such as stair-stepping and periodic striping in the
            data which may not be apparent when viewing the elevation data but might affect derivative products.
            They are not suitable for continuous data
**average**: average computes the average of all non-NODATA contributing pixels
**mode**: selects the value which appears most often of all the sampled points


## COGS-Conversion Command Options:

### Requirements:
Before using cog conversion command options, run the following:

```bash
  $ module use /g/data/v10/public/modules/modulefiles/
  $ module load dea
```

### Command: `convert`

 Convert a single or list of NetCDF files into Cloud Optimise GeoTIFF format.
 Uses a configuration file to define the file naming schema.

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py convert --help
Usage: cog_conv_app.py convert [OPTIONS]

  Convert a single/list of files into COG format

Options:
  -p, --product-name TEXT  Product name as defined in product configuration file  [required]
  -o, --output-dir PATH    Output destination directory  [required]
  -c, --config TEXT        Product configuration file
  -l, --filelist TEXT      List of input file names
  --help                   Show this message and exit.

```

#### Example
```bash
python3 converter/cog_conv_app.py convert -p wofs_albers -o /temp/output/dir -l /tmp/wofs_albers_nc_fllist.txt
```


### Command: `save-s3-inventory`

Scan through S3 bucket for the specified product and fetch the file path of the uploaded files.
Save those file into a pickle file for further processing.
Uses a configuration file to define the file naming schema.

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py save-s3-inventory --help
Usage: cog_conv_app.py save-s3-inventory [OPTIONS]

  Save S3 inventory list in a pickle file

Options:
  -p, --product-name TEXT        Product name as defined in product configuration file  [required]
  -o, --output-dir PATH          Output destination directory  [required]
  -c, --config TEXT              Product configuration file
  -i, --inventory-manifest TEXT  The manifest of AWS S3 bucket inventory URL
  --aws-profile TEXT             AWS profile name
  --help                         Show this message and exit.

```

#### Example
```bash
python3 converter/cog_conv_app.py save-s3-inventory -p wofs_albers -o /outdir/ --aws-profile tempProfile
```


### Command: `generate-work-list`

Compares datacube file uri's against S3 bucket (file names within pickle file) and writes the list of datasets
for cog conversion into the task file.
Uses a configuration file to define the file naming schema.

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py generate-work-list --help
Usage: cog_conv_app.py generate-work-list [OPTIONS]

  Generate task list for COG conversion

Options:
  -p, --product-name TEXT  Product name as defined in product configuration file  [required]
  -o, --output-dir PATH    Output destination directory  [required]
  --pickle-file TEXT       Pickle file containing the list of s3 bucket inventory  [required]
  --time-range TEXT        The time range:
                           '2018-01-01 < time < 2018-12-31'  OR
                           'time in 2018-12-31'  OR
                           'time=2018-12-31'  [required]
  -c, --config TEXT        Product configuration file
  --help                   Show this message and exit.

```

#### Example
```bash
python3 converter/cog_conv_app.py generate-work-list -p wofs_albers -o /outdir/ 
--pickle-file /dir/ls7_fc_albers_s3_inv_list.pickle --time-range 'time in 2018-12 '
```


### Command: `mpi-convert`

Bulk COG Convert netcdf files to COG format using MPI tool.
Iterate over the file list and assign MPI worker for processing.
Split the input file by the number of workers, each MPI worker completes every nth task.
Also, detect and fail early if not using full resources in an MPI job.

File naming schema is read from the configuration file.

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py mpi-convert --help
Usage: cog_conv_app.py mpi-convert [OPTIONS] FILELIST

  Bulk COG conversion using MPI

Options:
  -p, --product-name TEXT  Product name as defined in product configuration file  [required]
  -o, --output-dir PATH    Output destination directory  [required]
  -c, --config TEXT        Product configuration file
  --help                   Show this message and exit.

```

#### Example
```
#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=6200GB,ncpus=1600
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify PRODUCT, OUTPUT_DIR, and FILE_LIST using qsub -v option
## eg qsub -v PRODUCT=ls7_fc_albers,OUTPUT_DIR=/outdir/ls7,FILE_LIST=/outdir/ls7_fc_albers.txt run_cogger.sh

set -xe

source "$HOME/.bashrc"
module use /g/data/v10/public/modules/modulefiles/
module load dea
module load openmpi/3.1.2

mpirun --tag-output dea-cogger mpi-convert -p "${PRODUCT}" -o "${OUTPUT_DIR}" "${FILE_LIST}"

```


### Command: `qsub-cog`

Submits an COG conversion job, using a five stage PBS job submission.
Uses a configuration file to define the file naming schema.

Stage 1 (Store S3 inventory list to a pickle file):
    1) Scan through S3 inventory list and fetch the uploaded file names of the desired product
    2) Save those file names in a pickle file

Stage 2 (Generate work list for COG conversion):
       1) Compares datacube file uri's against S3 bucket (file names within pickle file)
       2) Write the list of datasets not found in S3 to the task file
       3) Repeat until all the datasets are compared against those found in S3

Stage 3 (Bulk COG convert using MPI):
       1) Iterate over the file list and assign MPI worker for processing.
       2) Split the input file by the number of workers, each MPI worker completes every nth task.

Stage 4 (Validate COG GeoTIFF files):
        1) Validate GeoTIFF files and if valid then upload to S3 bucket

Stage 5 (Run AWS sync to upload files to AWS S3):
        1) Using aws sync command line tool, sync newly COG converted files to S3 bucket

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py qsub-cog --help
Usage: cog_conv_app.py qsub-cog [OPTIONS]

  Kick off five stage COG Conversion PBS job

Options:
  -p, --product-name TEXT        Product name as defined in product configuration file  [required]
  --s3-output-url TEXT           S3 URL for uploading the converted files  [required]
  -o, --output-dir PATH          Output destination directory  [required]
  --time-range TEXT              The time range:
                                 '2018-01-01 < time < 2018-12-31'  OR
                                 'time in 2018-12-31'  OR
                                 'time=2018-12-31'  [required]
  -i, --inventory-manifest TEXT  The manifest of AWS S3 bucket inventory URL
  --aws-profile TEXT             AWS profile name
  --help                         Show this message and exit.

```

#### Example
```
python3 converter/cog_conv_app.py qsub-cog --s3-output-url 's3://dea-public-data/' -p wofs_albers 
--time-range '2018-11-30 < time < 2018-12-01' --output-dir /tmp/wofls_cog/
```


### Command: `verify`

    Verify converted GeoTIFF files are (Geo)TIFF with cloud optimized compatible structure.
    Mandatory Requirement: `validate_cloud_optimized_geotiff.py` gdal file.

#### Usage
```
> [COG-Conversion]$ python3 converter/cog_conv_app.py verify --help
Usage: cog_conv_app.py verify [OPTIONS] PATH

  Verify GeoTIFFs are Cloud Optimised GeoTIFF

Options:
  --rm-broken  Remove directories with broken files
  --help       Show this message and exit.

```

#### Example
```
#!/bin/bash
#PBS -l wd,walltime=5:00:00,mem=186GB,ncpus=48
#PBS -P v10
#PBS -q normal
#PBS -lother=gdata1:gdata2
#PBS -W umask=33
#PBS -m abe -M nci.monitor@dea.ga.gov.au

## specify OUTPUT_DIR using qsub -v option
## eg qsub -v OUTPUT_DIR=/outdir/ls7 run_verify.sh

set -xe

source "$HOME/.bashrc"
module use /g/data/v10/public/modules/modulefiles/
module load dea
module load openmpi/3.1.2

mpirun --tag-output dea-cogger verify --rm-broken "${OUTPUT_DIR}"
```


### Validate Cog Files

    Validate the GeoTIFFs using the GDAL script.

#### Usage
```
> $ python3 validate_cloud_optimized_geotiff.py --help
Usage: validate_cloud_optimized_geotiff.py [-q] test.tif

```

# Cloud Optimized GeoTIFF Summary

#### Cloud Optimized GeoTIFF rely on two complementary pieces of technology.

    The first is the ability of GeoTIFF's to store not just the raw pixels of the image, but to organize those pixels
    in particular ways. The second is HTTP GET range requests, that let clients ask for just the portions of a file
    that they need. Using the first organizes the GeoTIFF so the latter's requests can easily select the parts of the
    file that are useful for processing.

## GeoTIFF Organization

     The two main organization techniques that Cloud Optimized GeoTIFF's use are Tiling and Overviews.
     And the data is also compressed for more efficient passage online.

     Tiling creates a number of internal `tiles` inside the actual image, instead of using simple `stripes` of data.
     With a stripe of data then the whole file needs to be read to get the key piece.
     With tiles much quicker access to a certain area is possible, so that just the portion of the file that needs to
     be read is accessed.

     Overviews create down sampled versions of the same image. This means it's `zoomed out` from the original image
     - it has much less detail (1 pixel where the original might have 100 or 1000 pixels), but is also much smaller.
     Often a single GeoTIFF will have many overviews, to match different zoom levels. These add size to the overall
     file, but are able to be served much faster, since the renderer just has to return the values in the overview
     instead of figuring out how to represent 1000 different pixels as one.

     These, along with compression of the data, are general best practices for enabling software to quickly access
     imagery. But they are even more important to enable the HTTP GET Range requests to work efficiently.

## HTTP Get Range requests

    HTTP Version 1.1 introduced a very cool feature called Range requests. It comes into play in GET requests,
    when a client is asking a server for data. If the server advertises with an Accept-Ranges: bytes header in its
    response it is telling the client that bytes of data can be requested in parts, in whatever way the client wants.
    This is often called Byte Serving.
    The client can request just the bytes that it needs from the server.
    On the broader web it is very useful for serving things like video, so clients don't have to download
    the entire file to begin playing it.

    The Range requests are an optional field, so web servers are not required to implement it.
    But most all the object storage options on the cloud (Amazon, Google, Microsoft, OpenStack etc) support the field on
    data stored on their servers. So most any data that is stored on the cloud is automatically able to serve up parts
    of itself, as long as clients know what to ask for.

## Bringing them together

    Describing the two technologies probably makes it pretty obvious how the two work together.
    The Tiling and Overviews in the GeoTIFF put the right structure on the files on the cloud so that the Range queries
    can request just the part of the file that is relevant.

    Overviews come into play when the client wants to render a quick image of the whole file - it doesn't have to
    download every pixel, it can just request the much smaller, already created, overview. The structure of the GeoTIFF
    file on an HTTP Range supporting server enables the client to easily find just the part of the whole file that
    is needed.

    Tiles come into play when some small portion of the overall file needs to be processed or visualized.
    This could be part of an overview, or it could be at full resolution. But the tile organizes all the relevant bytes
    of an area in the same part of the file, so the Range request can just grab what it needs.

    If the GeoTIFF is not cloud optimized with overviews and tiles then doing remote operations on the data will
    still work. But they may download the whole file or large portions of it when only a very small part of the
    data is actually needed.`python3 validate_cloud_optimized_geotiff.py /tmp/x_0/y_-20/2018/11/30/LS_WATER_3577_0_-20_water.tif``

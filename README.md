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
    data is actually needed.


# NETCDF-COG conversion
  NetCDF to COG conversion from NCI file system

   - Convert the netcdfs that are on NCI /g/data/ file system and save them to the output path provided
   - Use `streamer.py to convert to COG:

#### Usage

```
> $python3 streamer/streamer.py --help
Usage: streamer.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  cog-convert       Convert a single/list of NetCDF files into COG format
  inventory-store   Store S3 inventory list in a pickle file
  list-datasets     Generate task list for COG conversion
  mpi-cog-convert   Parallelise COG convert using MPI
  qsub-cog-convert  Kick off four stage COG Conversion PBS job
  verify-cog-files  Verify the converted GeoTIFF are Cloud Optimised GeoTIFF

```

##### Example of a Yaml file:
```
    products:
        product_name:
            prefix: WOfS/WOFLs/v2.1.5/combined
            name_template: x_{x}/y_{y}/{time:%Y}/{time:%m}/{time:%d}/LS_WATER_3577_{x}_{y}_{time:%Y%m%d%H%M%S%f}
            stacked_name_template: x_{x}/y_{y}/{time:%Y}/{time:%m}/{time:%d}/LS_WATER_3577_{x}_{y}_{time:%Y}_
            predictor: 2
            nonpym_list: ["source", "observed"]
            default_rsp: average
            white_list: None
            black_list: None
where,
      product_name:              A unique user defined string (required)
      prefix:                    Define the cogs folder structure and name (required)
      name_template:             Define how to decipher the input file names (required)
      stacked_name_template:     Define how to decipher the stacked input file names (required)
      default_rsp:               Define the resampling method of pyramid view (default: average)
      predictor:                 Define the predictor in COG convert (default: 2)
      nonpym_list:               A list of keywords of bands which don't require resampling(optional)
      white_list:                A list of keywords of bands to be converted (optional)
      black_list:                A list of keywords of bands excluded in cog convert (optional)

Note: `nonpym_list` contains the key words of the band names which one doesn't want to generate overviews.
      This element cannot be used with other products as this 'cause it will match as *source*'.
      For most products, this element is not needed. So far, only fractional cover percentile use this.
```
##### What to set for predictor and resampling:

```
Predictor
<int> (1=default, 2=Horizontal differencing, 3 =floating point prediction)
**Horizontal differencing** is particularly useful for 16-bit data when the high-order and low-order bytes are
changing at different frequencies.Predictor=2 option should improve compression on any files greater than 8 bits/resel.
The **floating point** predictor PREDICTOR=3 results in significantly better compression ratios for floating point data.
There doesn't seem to be a performance penalty either for writing data with the floating point predictor, so it's a
pretty safe bet for any Float32 data.

Raster Resampling
*default_rsp* <resampling method> (average, nearest, mode)
**nearest**: Nearest neighbor has a tendency to leave artifacts such as stair-stepping and periodic striping in the
            data which may not be apparent when viewing the elevation data but might affect derivative products.
            They are not suitable for continuous data
**average**: average computes the average of all non-NODATA contributing pixels
**mode**: selects the value which appears most often of all the sampled points

```

## COGS-Conversion Command Options:

### Requirements:
Before using cog conversion command options, run the following:
  * $ module use /g/data/v10/public/modules/modulefiles/
  * $ module load dea

### Command: `cog-convert`

     Convert a single or list of NetCDF files into Cloud Optimise GeoTIFF format.
     Uses a configuration file to define the file naming schema.

#### Usage
```
> $python3 streamer/streamer.py cog-convert --help
Usage: streamer.py cog-convert [OPTIONS] [FILENAMES]...

  Convert a single/list of NetCDF files into COG format

Options:
  -p, --product-name TEXT  Product name as defined in product configuration file  [required]
  -c, --config TEXT        Product configuration file (Optional)
  -o, --output-dir TEXT    Output work directory (Optional)
  -l, --filelist TEXT      List of netcdf file names (Optional)
  --help                   Show this message and exit.

```

#### Example
`` python3 streamer/streamer.py cog-convert -p wofs_albers /-1_-12/test_-1_-12_2018100_v1546165254.nc
                OR
   python3 streamer/streamer.py cog-convert -p wofs_albers -l /tmp/wofs_albers_nc_file_list.txt``


### Command: `inventory-store`

    Scan through S3 bucket for the specified product and fetch the file path of the uploaded files.
    Save those file into a pickle file for further processing.
    Uses a configuration file to define the file naming schema.

#### Usage
```
> $python3 streamer/streamer.py inventory-store --help
Usage: streamer.py inventory-store [OPTIONS]

  Store S3 inventory list in a pickle file

Options:
  -p, --product-name TEXT        Product name as defined in product configuration file  [required]
  -c, --config TEXT              Product configuration file (Optional)
  -o, --output-dir TEXT          Output work directory (Optional)
  -i, --inventory-manifest TEXT  The manifest of AWS S3 bucket inventory URL (Optional)
  --aws-profile TEXT             AWS profile name (Optional)
  --help                         Show this message and exit.

```

#### Example
`` python3 streamer/streamer.py inventory-store -p wofs_albers --aws-profile tempProfile -o /tmp/``


### Command: `list-datasets`

    Compares datacube file uri's against S3 bucket (file names within pickle file) and writes the list of datasets
    for cog conversion into the task file.
    Uses a configuration file to define the file naming schema.

#### Usage
```
> $python3 streamer/streamer.py list-datasets --help
Usage: streamer.py list-datasets [OPTIONS]

  Generate task list for COG conversion

Options:
  -p, --product-name TEXT        Product name as defined in product configuration file  [required]
  --time-range TEXT              The time range:
                                 '2018-01-01 < time < 2018-12-31'  OR
                                 'time in 2018-12-31'  OR
                                 'time=2018-12-31'  [required]
  -c, --config TEXT              Product configuration file (Optional)
  -o, --output-dir TEXT          Output work directory (Optional)
  -E, --datacube-env TEXT        Datacube environment (Optional)
  -i, --inventory-manifest TEXT  The manifest of AWS S3 bucket inventory URL (Optional)
  --aws-profile TEXT             AWS profile name (Optional)
  --s3-list TEXT                 Pickle file containing the list of s3 bucket inventory (Optional)
  --help                         Show this message and exit.

```

#### Example
``python3 streamer/streamer.py list-datasets -p wofs_albers --time-range '2018-11-30 < time < 2018-12-01'``


### Command: `mpi-cog-convert`

    Convert netcdf files to COG format using parallelisation by MPI tool.
    Iterate over the file list and assign MPI worker for processing.
    Following details how master and worker interact during MPI cog conversion process:
        1) Master fetches the file list from the task file.
        2) Master shall then assign tasks to worker with task status as 'READY' for cog conversion.
        3) Worker executes COG conversion algorithm and sends task status as 'START'.
        4) Once worker finishes COG conversion, it sends task status as 'DONE' to the master.
        5) If master has more work, then process continues as defined in steps 2-4.
        6) If no tasks are pending with master, worker sends task status as 'EXIT' and closes the communication.
        7) Finally master closes the communication.

    Uses a configuration file to define the file naming schema.

#### Usage
```
> $python3 streamer/streamer.py mpi-cog-convert --help
Usage: streamer.py mpi-cog-convert [OPTIONS] FILELIST

  Parallelise COG convert using MPI

Options:
  -p, --product-name TEXT  Product name as defined in product configuration file  [required]
  -c, --config TEXT        Product configuration file (Optional)
  -o, --output-dir TEXT    Output work directory (Optional)
  --help                   Show this message and exit.

```

#### Example
``
mpirun python3 streamer/streamer.py mpi-cog-convert -c aws_products_config.yaml
--output-dir /tmp/wofls_cog/ -p wofs_albers /tmp/wofs_albers_file_list``


### Command: `qsub-cog-convert`

    Submits an COG conversion job, using a four stage PBS job submission.
    Uses a configuration file to define the file naming schema.

    Stage 1 (Store S3 inventory list to a pickle file):
        1) Scan through S3 inventory list and fetch the uploaded file names of the desired product.
        2) Save those file names in a pickle file.

    Stage 2 (Generate work list for COG conversion):
           1) Compares datacube file uri's against S3 bucket (file names within pickle file).
           2) Write the list of datasets not found in S3 to the task file.
           3) Repeat until all the datasets are compared against those found in S3.

    Stage 3 (COG convert using MPI runs):
           1) Master fetches the file list from the task file.
           2) Master shall then assign tasks to worker with task status as 'READY' for cog conversion.
           3) Worker executes COG conversion algorithm and sends task status as 'START'.
           4) Once worker finishes COG conversion, it sends task status as 'DONE' to the master.
           5) If master has more work, then process continues as defined in steps 2-4.
           6) If no tasks are pending with master, worker sends task status as 'EXIT' and closes the communication.
           7) Finally master closes the communication.

    Stage 4 (Validate GeoTIFF files and run AWS sync to upload files to AWS S3):
            1) Validate GeoTIFF files and if valid then upload to S3 bucket.
            2) Using aws sync command line tool, sync newly COG converted files to S3 bucket.

#### Usage
```
> $python3 streamer/streamer.py qsub-cog-convert --help
Usage: streamer.py qsub-cog-convert [OPTIONS]

  Kick off four stage COG Conversion PBS job

Options:
  -p, --product-name TEXT         Product name as defined in product configuration file  [required]
  --time-range TEXT               The time range:
                                  '2018-01-01 < time < 2018-12-31'  OR
                                  'time in 2018-12-31'  OR
                                  'time=2018-12-31'  [required]
  -c, --config TEXT               Product configuration file (Optional)
  -o, --output-dir TEXT           Output work directory (Optional)
  -q, --queue                     [normal|express]
  -P, --project TEXT              Project Name
  -n, --nodes INTEGER RANGE       Number of nodes to request (Optional)
  -t, --walltime INTEGER RANGE    Number of hours (range: 1-48hrs) to request (Optional)
  -m, --email-options             [a|b|e|n|ae|ab|be|abe]
                                  Send email when execution is, 
                                  [a = aborted | b = begins | e = ends | n = do not send email]
  -M, --email-id TEXT             Email recipient id (Optional)
  -i, --inventory-manifest TEXT   The manifest of AWS S3 bucket inventory URL (Optional)
  --aws-profile TEXT              AWS profile name (Optional)
  -E, --datacube-env TEXT         Datacube environment (Optional)
  --help                          Show this message and exit.

```

#### Example
``python3 streamer/streamer.py qsub-cog-convert -q normal -P v10 -M temp@email.com -p wofs_albers 
--time-range '2018-11-30 < time < 2018-12-01'``


### Command: `verify-cog-files`

    Verify converted GeoTIFF files are (Geo)TIFF with cloud optimized compatible structure.
    Mandatory Requirement: `validate_cloud_optimized_geotiff.py` gdal file.

#### Usage
```
> $python3 streamer/streamer.py verify-cog-files --help
Usage: streamer.py verify-cog-files [OPTIONS]

  Verify the converted GeoTIFF are Cloud Optimised GeoTIFF

Options:
  -p, --path PATH  Validate the GeoTIFF files from this folder  [required]
  --help           Show this message and exit.

```

#### Example
`` python3 streamer/streamer.py verify-cog-files --path /tmp/wofls_cog``


### Validate Cog Files

    Validate the GeoTIFFs using the GDAL script.

#### Usage
```
> $ python3 validate_cloud_optimized_geotiff.py --help
Usage: validate_cloud_optimized_geotiff.py [-q] test.tif

```

#### Example
``python3 validate_cloud_optimized_geotiff.py /tmp/x_0/y_-20/2018/11/30/LS_WATER_3577_0_-20_water.tif``

# Cloud Optimized GeoTIFF Summary

#### Cloud Optimized GeoTIFF rely on two complementary pieces of technology.

    The first is the ability of GeoTIFF's to store not just the raw pixels of the image, but to organize those pixels in particular         ways. 
    The second is HTTP GET range requests, that let clients ask for just the portions of a file that they need. Using the first organizes the GeoTIFF so the latter's requests can easily select the parts of the file that are useful for processing.

## GeoTIFF Organization

     The two main organization techniques that Cloud Optimized GeoTIFF's use are Tiling and Overviews.
     And the data is also compressed for more efficient passage online.

     Tiling creates a number of internal `tiles` inside the actual image, instead of using simple `stripes` of data.
     With a stripe of data then the whole file needs to be read to get the key piece.
     With tiles much quicker access to a certain area is possible, so that just the portion of the file that needs to
     be read is accessed.

     Overviews create down sampled versions of the same image. This means it's `zoomed out` from the original image -
     it has much less detail (1 pixel where the original might have 100 or 1000 pixels), but is also much smaller.
     Often a single GeoTIFF will have many overviews, to match different zoom levels. These add size to the overall file,
     but are able to be served much faster, since the renderer just has to return the values in the overview instead of
     figuring out how to represent 1000 different pixels as one.

     These, along with compression of the data, are general best practices for enabling software to quickly access imagery.
     But they are even more important to enable the HTTP GET Range requests to work efficiently.

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
    data stored on their servers. So most any data that is stored on the cloud is automatically able to serve up parts of
    itself, as long as clients know what to ask for.

## Bringing them together

    Describing the two technologies probably makes it pretty obvious how the two work together.
    The Tiling and Overviews in the GeoTIFF put the right structure on the files on the cloud so that the Range queries
    can request just the part of the file that is relevant.

    Overviews come into play when the client wants to render a quick image of the whole file - it doesn't have to download
    every pixel, it can just request the much smaller, already created, overview. The structure of the GeoTIFF file on an
    HTTP Range supporting server enables the client to easily find just the part of the whole file that is needed.

    Tiles come into play when some small portion of the overall file needs to be processed or visualized.
    This could be part of an overview, or it could be at full resolution. But the tile organizes all the relevant bytes
    of an area in the same part of the file, so the Range request can just grab what it needs.

    If the GeoTIFF is not cloud optimized with overviews and tiles then doing remote operations on the data will still work.
    But they may download the whole file or large portions of it when only a very small part of the data is actually needed.


# NETCDF- COG conversion
 NetCDF to COG conversion from NCI file system

- Convert the netcdfs that are on NCI g/data file system and save them to the output path provided
- Use `streamer.py to convert to COG:

```
> $python3 streamer/streamer.py  --help 
Usage: streamer.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  convert-cog         Convert a list of NetCDF files into Cloud Optimise...
  generate-work-list  Connect to an ODC database and list NetCDF files
  mpi-convert-cog     parallelize the COG convert with MPI.

```
## convert-cog

To Convert the netcdfs to COG
```
> $python3 streamer/streamer.py convert-cog  --help 
Usage: streamer.py convert-cog [OPTIONS] [FILENAMES]...

  Convert a list of NetCDF files into Cloud Optimise GeoTIFF format

  Uses a configuration file to define the file naming schema.

Options:
  -c, --config TEXT  Config file
  --output-dir TEXT  Output directory  [required]
  --product TEXT     Product name  [required]
  -l, --flist TEXT   List of file names
  --help             Show this message and exit.
```

Description:

	--config | -c ``$config_yaml_file``: load configurations from *YAML* file
	--output-dir ``$output_dir``: specify the path where the *COGS* will be written
	--product ``$product_name``: specify a product name declared in config yaml file ``$product_name``
	--flist | -f ``$file_list``: load the file names in ``$file_list``, not used together with``$file``
    
Example of a Yaml file:

```
	products:
		fcp_seasonal:
			dest_template: x_{x}/y_{y}/{year}{month}
			src_template: whatever_{x}_{y}_{start}_{end}
			default_rsp: average
			nonpym_list: ["source", "observed"]
		fcp_annual:
			dest_template: x_{x}/y_{y}/{year}
			src_template: whatever_{x}_{y}_{start}
			default_rsp: average
			nonpym_list: ["source", "observed"]
		wofls:
			dest_template: x_{x}/y_{y}/{year}/{month}/{day}
			src_template: whatever_{x}_{y}_{time}
			default_rsp: nearest
			predictor: 2
```

Config formatting to include:

```
 products:
        $product_name:     #a unique user defined string
            dest_template:     #define the cogs folder structure and name (required)
            src_template:      #define how to decipher the input file names (required)
            default_rsp:       #define the resampling method of pyramid view (optional default: average)
            predictor:         #define the predictor in COG convert (optional default: 2)
            nonpym_list:       #a list of keywords of bands which don't require resampling(optional)
            white_list:        #a list of keywords of bands to be converted (optional)
            black_list:        #a list of keywords of bands excluded in cog convert (optional)
```
What to set for predictor and resampling:

```
Predictor
<int> (1=default, 2=Horizontal differencing, 3 =floating point prediction)
**Horizontal differencing** is particularly useful for 16-bit data when the high-order and low-order bytes are
changing at different frequencies.Predictor=2 option should improve compression on any files greater than 8 bits/resel.
The **floating point** predictor PREDICTOR=3 results in significantly better compression ratios for floating point data.
There doesn't seem to be a performance penalty either for writing data with the floating point predictor, so it's a pretty safe bet for any Float32 data.

Raster Resampling
*default_rsp* <resampling method> (average, nearest, mode)
**nearest**: Nearest neighbor has a tendency to leave artifacts such as stair-stepping and periodic striping in the data which m
ay not be apparent when viewing the elevation data but might affect derivative products. They are not suitable for continuous data
**average**: average computes the average of all non-NODATA contributing pixels
**mode**: selects the value which appears most often of all the sampled points

```

Example of converting COGS:

- Run as a single process:


```
    python $path_to_script/streamer.py convert_cog -c cog.yaml --output-dir $output_dir --product $product_name -l $file-list
```
## mpi-convert-cog

  Convert To COG using parallelization by MPI tool

####Requirements:

* openmpi >= 3.0.0 (module load openmpi/3.0.0)
* mpi4py (pip install mpi4py)
```
>  python3 streamer/streamer.py mpi-convert-cog  --help 
Usage: streamer.py mpi-convert-cog [OPTIONS] FILELIST

  parallelize the COG convert with MPI.

Options:
  -c, --config TEXT   Config file
  --output-dir TEXT   Output directory  [required]
  --product TEXT      Product name  [required]
  --numprocs INTEGER  Number of processes  [required]
  --cog-path TEXT     cog convert script path  [required]
  --help              Show this message and exit.
```

Description:
	--config | -c `$config_yaml_file`: load configurations from *YAML* file
	--output-dir `$output_dir`: specify the path where the *COGS* will be written
	--product `$product_name`: product name defined in `$config_yaml_file`
	--flist | -f `$file_list`: load the file names in `$file_list`, not used together with `$file`
	--numprocs `$int`: number of processes when parallelized with *MPI*, usually the number should be `$int = $number_of_cpus - 1`
	--cog-path `$script_to_run`: the script to run for each *MPI* process, now it should be the same as `$path_to_script/streamer.py`
	as everything is in the sample python script


Command to run:

```    mpirun --oversubscribe -n 1 python $path_to_script/streamer.py mpi_convert_cog -c cog.yaml --output-dir $ouput_dir --product $product_name --numprocs 63 --cog-path $path_to_script/streamer.py $file_list
```
Note: the total number of CPUS is 64 over 4 nodes.


## generate-work-list

The simple way to get the file list is to do

`find $path_to_netcdfs -name "*.nc" > path_to_file.list`
or run `generate-work-list`. You need to set `datacube.conf` to run the following command 
```
> $python3 streamer/streamer.py generate-work-list --help 
Usage: streamer.py generate-work-list [OPTIONS]

  Connect to an ODC database and list NetCDF files

Options:
  -p, --product-name TEXT  Product name  [required]
  -y, --year INTEGER       The year
  -m, --month INTEGER      The month
  --help                   Show this message and exit.
```

# Geotiff- COG conversion
 geotiff to cog conversion from NCI file system  
 Checkout `old-style` branch from the repo and use `geotiff-cog.py`
- Convert the Geotiff that are on NCI g/data file system and save them to the output path provided 
- To use python script to convert Geotiffs to COG data:

```
> $ python geotiff-cog.py --help

  Usage: geotiff-cog.py [OPTIONS]

  Convert Geotiff to Cloud Optimized Geotiff using gdal. Mandatory
  Requirement: GDAL version should be >=2.2

  Options:
    -p, --path PATH    Read the Geotiffs from this folder  [required]
    -o, --output PATH  Write COG's into this folder  [required]
    --help             Show this message and exit.
```

# Validate the Geotiffs using the GDAL script
- How to use the Validate_cloud_Optimized_Geotiff:  
```
> $ python validate_cloud_optimized_geotiff.py --help  

Usage: validate_cloud_optimized_geotiff.py [-q] test.tif  

```
# To verify all GeoTIFF's, run the script:
```
> $python verify_cog.py --help

  Usage: verify_cog.py [OPTIONS]

  Verify the converted Geotiffs are Cloud Optimized Geotiffs. Mandatory
  Requirement: validate_cloud_optimized_geotiff.py gdal file

Options:
  -p, --path PATH  Read the Geotiffs from this folder  [required]
  --help           Show this message and exit.
```


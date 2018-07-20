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
- To use python script to convert to COG:

```
> $ python netcdf-cog.py --help

Usage: netcdf-cog.py [OPTIONS]

  Convert netcdf to Geotiff and then to Cloud Optimized Geotiff using
  gdal. Mandatory Requirement: GDAL version should be >=2.2

Options:
  -p, --path PATH       Read the netcdfs from this folder  [required]
  -o, --output PATH     Write COG's into this folder  [required]
  -s, --subfolder TEXT  Subfolder for this task
  --help                Show this message and exit.

```

# Geotiff- COG conversion
 geotiff to cog conversion from NCI file system  
 
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

# Upload data to AWS S3 Bucket

- Run the compute_sync.sh BASH script under the compute-sync folder as a PBS job and update more profile use case

- To run the script/ submit job - qsub compute-sync.sh

- Usage:
  ``` aws s3 sync {from_folder} {to_folder} --includes {include_specific_files} --excludes {exclude_specific_extension_files}
      {from_folder} : Will sync all the folders, subfolders and files in the given path excluding the path to foldername
      {to_folder} : Provide S3 URL as in s3://{bucket_name}/{object_path}. If the object path is not present the path specified
                    in {from_folder} is duplicated in S3 bucket
       --include (string) Don't exclude files or objects in the command that match the specified pattern.
       --exclude (string) Exclude all files or objects from the command that matches the specified pattern.

  ```

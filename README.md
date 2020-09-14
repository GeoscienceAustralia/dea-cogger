# NetCDF to Cloud Optimised GeoTIFF Conversion Tool

Perform NetCDF to COG conversion for synchronising data from NCI file systems to storing on AWS S3.

Convert the NetCDFs that are on NCI `/g/data/` file system and save them 
  to the output path.

Use `dea-cogger` convert to COG:

## Usage


### Example configuration file

```
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

- **product_name**:              A unique user defined string (required)
- **prefix**:                    Define the cogs folder structure and name (required)
- **name_template**:             Define how to decipher the input file names (required)
- **default_resampling**:        Define the resampling method of pyramid view (default: average)
- **predictor**:                 Define the predictor in COG convert (default: 2)
- **no_overviews**:              A list of keywords of bands which don't require resampling (optional)
- **white_list**:                A list of keywords of bands to be converted (optional)
- **black_list**:                A list of keywords of bands excluded in cog convert (optional)

Note: `no_overviews` contains the key words of the band names which one doesn't want to generate overviews.
      This element cannot be used with other products as this 'cause it will match as *source*'.
      For most products, this element is not needed. So far, only fractional cover percentile use this.
      
### What to set for predictor and resampling:

**Predictor**

<int> (1=default, 2=Horizontal differencing, 3 =floating point prediction)

- **Horizontal differencing**

  particularly useful for 16-bit data when the high-order and low-order bytes are changing at different frequencies.

  Predictor=2 option should improve compression on any files greater than 8 bits/resel.

- **Floating Point Prediction**
  The **floating point** predictor PREDICTOR=3 results in significantly better compression ratios for floating point data.
  
  There doesn't seem to be a performance penalty either for writing data with the floating point predictor, so it's a
  pretty safe bet for any Float32 data.

**Raster Resampling**

*default_resampling* <resampling method> (average, nearest, mode)

- **nearest**: Nearest neighbor has a tendency to leave artifacts such as stair-stepping and periodic striping in the
            data which may not be apparent when viewing the elevation data but might affect derivative products.
            They are not suitable for continuous data
            
- **average**: average computes the average of all non-NODATA contributing pixels
- **mode**: selects the value which appears most often of all the sampled points



### Command: `convert`

 Convert a single or list of NetCDF files into Cloud Optimise GeoTIFF format.
 Uses a configuration file to define the file naming schema.



### Command: `save-s3-inventory`

Scan through S3 bucket for the specified product and fetch the file path of the uploaded files.
Save those file into a pickle file for further processing.
Uses a configuration file to define the file naming schema.


### Command: `generate-work-list`

Compares ODC URI's against an S3 bucket  and writes the list of datasets
for COG conversion into a file.

Uses a configuration file to define the file naming schema.


### Command: `mpi-convert`

Bulk COG Convert netcdf files to COG format using MPI tool.
Iterate over the file list and assign MPI worker for processing.
Split the input file by the number of workers, each MPI worker completes every nth task.
Also, detect and fail early if not using full resources in an MPI job.

Reads the file naming schema from the configuration file.



### Command: `verify`

Verify converted GeoTIFF files are (Geo)TIFF with cloud optimized compatible structure.
Mandatory Requirement: `validate_cloud_optimized_geotiff.py` gdal file.



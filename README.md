# Geotiff-conversion
 geotiff to cog conversion from NCI file system  
 
- Convert the Geotiff that are on NCI g/data file system and save them to the output path provided 
- How to use th python script to convert Geotiffs to COG data:
```
> $ python geotiff-cog.py --help

  Usage: geotiff-cog.py [OPTIONS]

  Convert Geotiff to Cloud Optimized Geotiff using gdal. Mandatory
  Requirement: GDAL version should be <=2.2

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

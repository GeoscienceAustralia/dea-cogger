import subprocess
import click
import sys,os
import logging

@click.command(help= "\b Verify the converted Geotiffs are Cloud Optimized Geotiffs."
" Mandatory Requirement: validate_cloud_optimized_geotiff.py gdal file")
@click.option('--path', '-p', required = True, help="Read the Geotiffs from this folder",
                type=click.Path(exists=True, readable=True))
def main(path):
    Gtiff_path = os.path.abspath(path)
    count=0
    for root, subdirs,files in os.walk(Gtiff_path):
       for filename in files:
          if filename.endswith('.tif'):
              count = count+1
              file_name =os.path.join(root,filename)
              command = "python validate_cloud_optimized_geotiff.py " + file_name
              output = subprocess.getoutput(command)
              valid_output = (output.split('/'))[-1]
              print(str(count)+":"+valid_output)

if __name__ == "__main__":
    main()

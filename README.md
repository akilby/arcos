# arcos #
Code to process the DEA's publicly-available ARCOS Retail Drug Summary Reports

## How to Install ##

* Clone this repo to disk, then 

`pip install .`

NOTE: currently this only works if you `pip install -e .` (otherwise submodules don't work)	

## Command Line Tools ##

* This package comes with command line tools that can be used to download PDFs and construct the datasets from source.

`arcos download --folder /path/to/download/folder/` 

`arcos build --source-folder /path/to/download/folder/ --destination-folder /path/to/destination/folder`

The first command will download all ARCOS reports publicly available on the DEA website (currently around 70 PDFs).

The second command will process and build clean data files from the PDFs. The final output will be 6 data files, one for each report type. The files are saved in .dta for portability to Stata; they can be read with pandas read_stata or R with readstata13.

Note that while package can handle processing only a subset of the data/reports, currently if using the command line tools the best way to build only a subset of years or reports is to create a source folder containing only the relevant PDFs.

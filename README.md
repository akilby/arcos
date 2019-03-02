# arcos #
Code to process the DEA's publicly-available ARCOS Retail Drug Summary Reports

## How to Install ##

* Clone this repo to disk, then 

`pip install .`

## Command Line Tools ##

* This package comes with command line tools that can be used to download PDFs and construct the datasets from source.

`arcos download --folder /path/to/download/folder/` 
`arcos build --source-folder /path/to/download/folder/ --data-folder /path/to/data/folder`

The first command will download all ARCOS reports publicly available on the DEA website (approximately 70 PDFs).

The second command will process and build clean data files from the PDFs. The final output will be 6 data files, one for each report type. The files are saved in .dta for portability to Stata; they can also be read with pandas read_stata().
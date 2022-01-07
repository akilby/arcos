# arcos #
Code to process the DEA's publicly-available [ARCOS Retail Drug Summary Reports](https://www.deadiversion.usdoj.gov/arcos/retail_drug_summary/index.html).

ARCOS reports contain a census of all major controlled substances distributed in the United States every year, and are useful to researchers, but the data is housed in 1,000+ page PDF files with messy encoding that makes the data difficult to extract.

*2022-01-06: Package is updated and current as of the 2021 half-year report.*

## How to Install ##

Clone this repo to disk, then type:

`pip install .`

## Command Line Tools ##

This package comes with command line tools that can be used to download PDFs and construct the datasets from source.

`arcos download --folder /path/to/download/folder/` 

`arcos build --source-folder /path/to/download/folder/ --destination-folder /path/to/destination/folder`

The first command will download all ARCOS reports publicly available on the DEA website (currently around 70 PDFs).

The second command will process and build clean data files from the PDFs. The final output will be 6 data files, one for each report type. The files are saved in .dta for portability to Stata; they can be read with pandas read_stata or R with readstata13. There are also intermediate files that are stored in `_cached_data` that can be retrieved if the script fails.

This package can take a while to run. If you only want a subset of ARCOS reports, I recommend running the `arcos build` script on a source folder that only contains the reports that are relevant to you. (The `arcos download` routine downloads all publicly-available reports, so you may need to delete them.)

## Research and Citations ##

I use ARCOS data in the following papers, which also contain a more in-depth description of the data:

* Kilby, Angela (2021). [*Opioids for the masses: Welfare Tradeoffs in the Regulation of Narcotic Pain Medications*](https://angelakilby.com/pdfs/AKilbyWelfare_2022-01.pdf).
* Guo, Jiapei, Angela Kilby, and Mindy Marks (2021). [*The Impact of Scope-of-Practice Restrictions on Access to Medical Care*](https://angelakilby.com/pdfs/GuoKilbyMarksNursePractitioners_2022-01.pdf).
* Kilby, Angela (2021). *The Medicaid Expansion and Increased Access to Prescription Medications: Implications for the Opioid Overdose Crisis.*





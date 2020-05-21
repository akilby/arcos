
local report1path "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/data/Report1.dta"
local savepath "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/out/drug_zip3_quarter_master.dta" 

**********************************************************************************************************

* 0. Initial cleaning

**********************************************************************************************************

use `report1path', clear
gen quarter=qofd(dofc(QUARTER))
format quarter %tq
replace DRUG_NAME = "COCAINE" if DRUG_CODE=="9041"
drop if inlist(STATE, "AMERICAN SAMOA", "GUAM", "VIRGIN ISLANDS", "PUERTO RICO")
destring YEAR, replace


**********************************************************************************************************

* 1. METHADONE, 2016Q4 problem, fixing at state level using Report 5
* (zipcode level fix will need to just be interpolation)

**********************************************************************************************************


* Fix 2016q4 Methadone
* 2016 Q4 reporting for methadone distributed via N-U Narcotic Treatment Programs was not complete at the time the 2016
* ARCOS report was generated. As such, the value for 2016 Q4 methadone dispensed through these programs was 
* interpolated using the surrounding quarters. 


* Show it's still a problem:
preserve
	collapse (sum) GRAMS, by(quarter DRUG_NAME)	
	twoway connected GRAMS quarter if DRUG_NAME=="METHADONE" 
restore

* Fix 1 - methadone2
sort DRUG_NAME ZIP3 quarter
gen methadone2= (GRAMS[_n+1] + GRAMS[_n-1])/2 if DRUG_NAME =="METHADONE" & ZIP3==ZIP3[_n-1] & ZIP3==ZIP3[_n+1] & quarter==tq(2016q4)

replace GRAMS=methadone2 if missing(methadone2)==0
drop methadone2


**********************************************************************************************************

* 2. Merge in master drug-state-quarter list which includes drug categories
* I made a list of drugs that should have existed in the data for every state-year
* so if it's missing that means it's actually implied to be zero

**********************************************************************************************************

gen drug = DRUG_NAME + " - " + DRUG_CODE

preserve
	keep STATE ZIP3 quarter
	duplicates drop
	expand = 23
	bysort STATE ZIP3 quarter: gen n = _n
	merge m:1 n using "/Users/angelakilby/Dropbox/Research/Data/ARCOS/data/drug_categories.dta", assert(3) nogen
	drop n
	tempfile statezip3_quarter
	save `statezip3_quarter'
restore

merge 1:1 drug STATE ZIP3 quarter using `statezip3_quarter', 
tab DRUG_NAME if _merge==1
keep if _merge==2 | _merge==3
drop _merge

drop YEAR DRUG_CODE DRUG_NAME
replace GRAMS = 0 if missing(GRAMS)
split drug, parse(" - ")

ren drug drug_ext
ren drug1 drug
ren drug2 drug_code

**********************************************************************************************************

* 3 Merge in MMEs/conversion factors, adjust grams

**********************************************************************************************************


merge m:1 drug using "/Users/angelakilby/Dropbox/Research/Data/General/mme.dta", keep(1 3) nogen
merge m:1 drug using "/Users/angelakilby/Dropbox/Research/Data/General/conversion_factor.dta", keep(1 3) nogen
replace conversion_factor = 1 if drug=="NABILONE"
assert (missing(conversion_factor) & missing(mme))==0
assert (missing(conversion_factor)==0 & missing(mme)==0)==0


ren STATE state
ren ZIP3 zip3
ren GRAMS grams_rpt1
gen year = yofd(dofq(quarter))

gen grams_adj_rpt1 = grams_rpt1*conversion_factor if missing(conversion_factor)==0
replace grams_adj_rpt1 = grams_rpt1*mme if missing(mme)==0

drop QUARTER mme conversion_factor

* 3. Merge in population Controls, now with 2017 update

merge m:1 zip3 year using "/Users/angelakilby/Dropbox/Research/Data/Population/data/population_zip3_year_interpolated_fully.dta"
drop if _merge!=3
drop _merge

replace population=round(population)
* note also have: "/Users/angelakilby/Dropbox/Research/Data/Population/data/populations_acs_zip3_year.dta" 

* 4. Merge in unemployment controls

* don't have right now

save `savepath', replace


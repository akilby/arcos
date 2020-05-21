
*local report1path "/Users/angelakilby/Downloads/arcosTest2/Report1.dta"
*local report2path "/Users/angelakilby/Downloads/arcosTest2/Report2.dta"
*local report5path "/Users/angelakilby/Downloads/arcosTest2/Report5.dta"


local report1path "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/data/Report1.dta"
local report2path "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/data/Report2.dta"
local report5path "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/data/Report5.dta"

local savepath "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/out/drug_state_quarter_master.dta" 
local savepath_olddata "/Users/angelakilby/Dropbox/Research/Data/ARCOS_new/out/drug_state_quarter_master_olddata.dta" 

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

*replace GRAMS=methadone2 if missing(methadone2)==0
*drop methadone2

* Fix 2 - Using Report 5 at the state level
preserve
	use `report5path', clear
	destring YEAR, replace
	keep if DRUG_NAME=="METHADONE"
	sort STATE BUSINESS_ACTIVITY YEAR
	gen methadone3 = TOTAL_GRAMS
	replace methadone3 = (TOTAL_GRAMS[_n-1] + TOTAL_GRAMS[_n+1])/2 if YEAR==2016 & BUSINESS_ACTIVITY=="N-U NARCOTIC TREATMENT PROGRAMS"
	collapse (sum) TOTAL_GRAMS methadone3, by(STATE YEAR DRUG_NAME)
	tempfile rpt5
	save `rpt5'
restore


* Collapse to state-quarter-drug category level

collapse (sum) GRAMS methadone2, by(DRUG_NAME DRUG_CODE STATE YEAR quarter)
*tempfile bla
*save `bla'
replace methadone2 = . if (quarter==tq(2016q4) & DRUG_NAME=="METHADONE")==0
ren GRAMS grams_rpt1
sort DRUG_CODE DRUG_NAME STATE YEAR quarter

* Methadone from Report 5
merge m:1 STATE YEAR DRUG_NAME using `rpt5', keep(1 3) nogen
bysort STATE DRUG_NAME YEAR: egen totgrams = total(grams_rpt1) if DRUG_NAME=="METHADONE"
gen methadone4=(methadone3-totgrams)+grams_rpt1 if quarter==tq(2016q4)
drop TOTAL_GRAMS methadone3 totgrams

*gen grams_rpt1_fix2 = grams_rpt1 
*replace grams_rpt1_fix2 = methadone2 if missing(methadone2)==0
*gen grams_rpt1_fix4 = grams_rpt1 
*replace grams_rpt1_fix4 = methadone4 if missing(methadone4)==0



replace methadone2=grams_rpt1 if missing(methadone2)
replace methadone4=grams_rpt1 if missing(methadone4)

** For methadone in 2016q4, Report 5 source becomes the master, interpolation and original source become alts
ren methadone2 grams_rpt1_alt1
gen grams_rpt1_alt2 = grams_rpt1
assert missing(methadone4)==0
replace grams_rpt1 = methadone4 
drop methadone4

* Show it's not still a problem:
preserve
	collapse (sum) grams_rpt1 grams_rpt1_alt1 grams_rpt1_alt2, by(quarter DRUG_NAME)
	twoway connected grams_rpt1* quarter if DRUG_NAME=="METHADONE" & quarter>=tq(2006q1)
restore

// preserve
// 	keep if DRUG_NAME=="METHADONE"
// 	replace DRUG_NAME = "METHADONE-1"
// 	replace grams_rpt1 = grams_rpt1_alt1
// 	drop grams_rpt1_alt*
// 	ren DRUG_NAME drug
// 	ren DRUG_CODE drug_code
// 	ren YEAR year
// 	tempfile methadone1
// 	save `methadone1'
// restore	

// preserve
// 	keep if DRUG_NAME=="METHADONE"
// 	replace DRUG_NAME = "METHADONE-2"
// 	replace grams_rpt1 = grams_rpt1_alt2
// 	drop grams_rpt1_alt*
// 	ren DRUG_NAME drug
// 	ren DRUG_CODE drug_code
// 	ren YEAR year
// 	tempfile methadone2
// 	save `methadone2'
// restore	

drop grams_rpt1_alt*

*merge 1:1 DRUG_NAME DRUG_CODE STATE YEAR quarter using `bla'
*tab DRUG_NAME if abs(grams_rpt1-GRAMS)>.4

**********************************************************************************************************

* 2. Merge in master drug-state-quarter list which includes drug categories
* I made a list of drugs that should have existed in the data for every state-year
* so if it's missing that means it's actually implied to be zero

**********************************************************************************************************

gen drug = DRUG_NAME + " - " + DRUG_CODE

preserve
	keep STATE quarter
	duplicates drop
	expand = 23
	bysort STATE quarter: gen n = _n
	merge m:1 n using "/Users/angelakilby/Dropbox/Research/Data/ARCOS/data/drug_categories.dta", assert(3) nogen
	drop n
	tempfile state_quarter
	save `state_quarter'
restore

merge 1:1 drug STATE quarter using `state_quarter', 
tab DRUG_NAME if _merge==1
keep if _merge==2 | _merge==3
drop _merge

drop YEAR DRUG_CODE DRUG_NAME
replace grams_rpt1 = 0 if missing(grams_rpt1)
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


/*levelsof mme if drug=="METHADONE", local(methadone_mme)

preserve
	keep if drug=="OXYCODONE"
	replace category="Oxycodone"
	tempfile oxy
	save `oxy'
restore

preserve
	keep if drug=="HYDROCODONE"
	replace category="Hydrocodone"
	tempfile hyd
	save `hyd'
restore

preserve
	keep if drug=="BUPRENORPHINE"
	replace category="Buprenorphine"
	tempfile bup
	save `bup'
restore

preserve
	keep if drug=="METHADONE"
	replace category="Methadone"
	tempfile mth
	save `mth'
restore

append using `oxy'
append using `hyd'
append using `bup'
append using `mth'

append using `methadone1'
append using `methadone2'
replace category = "Methadone-1" if drug=="METHADONE-1"
replace category = "Methadone-2" if drug=="METHADONE-2"

replace grams_adj = grams_rpt1*`methadone_mme' if category == "Methadone-1" | category == "Methadone-2"

collapse (sum) grams_adj, by(category STATE quarter)
*/

ren STATE state

gen year = yofd(dofq(quarter))

tempfile rpt1
save `rpt1'


**********************************************************************************************************

* 2. Cross-check with Report 2

use `report2path', clear
replace DRUG_NAME = "COCAINE" if DRUG_CODE=="9041"

gen drug = DRUG_NAME + " - " + DRUG_CODE
gen quarter=qofd(dofc(QUARTER))
drop QUARTER
format quarter %tq
drop if inlist(STATE, "AMERICAN SAMOA", "GUAM", "VIRGIN ISLANDS", "PUERTO RICO")

*merge m:1 drug using "/Users/angelakilby/Dropbox/Research/Data/ARCOS/data/drug_categories.dta", keep(3) nogen keepusing(category)

merge 1:1 drug STATE quarter using `state_quarter', 
tab DRUG_NAME if _merge==1
keep if _merge==2 | _merge==3
drop _merge

drop YEAR DRUG_CODE DRUG_NAME
ren GRAMS grams_rpt2

replace grams_rpt2 = 0 if missing(grams_rpt2)
split drug, parse(" - ")

ren drug drug_ext
ren drug1 drug
ren drug2 drug_code

merge m:1 drug using "/Users/angelakilby/Dropbox/Research/Data/General/mme.dta", keep(1 3) nogen
merge m:1 drug using "/Users/angelakilby/Dropbox/Research/Data/General/conversion_factor.dta", keep(1 3) nogen
replace conversion_factor = 1 if drug=="NABILONE"
assert (missing(conversion_factor) & missing(mme))==0
assert (missing(conversion_factor)==0 & missing(mme)==0)==0

ren STATE state

merge 1:1 category state quarter drug using `rpt1'
drop _merge

*a problem in report 2 also, so fix
preserve
	collapse (sum) grams_rpt1 grams_rpt2, by(quarter drug)	
	twoway connected grams_rpt1 grams_rpt2 quarter if drug=="METHADONE" 
restore
replace grams_rpt2=grams_rpt1 if drug=="METHADONE"

gen e = (grams_rpt2-grams_rpt1)/grams_rpt1
*table drug year if e==-1 & missing(e)==0
table drug year if e>.07 & missing(e)==0
table drug year if e>-1 & e<-0.07 & missing(e)==0


* FIX: 2012 state data is missing some stuff; use zip3 data for that
*assert grams_rpt2==0 if e==-1
*replace grams_rpt2 = grams_rpt1 if e==-1
*replace grams_adj_rpt2 = grams_adj_rpt1 if e==-1

* FIX: small rounding
gen e2 = abs(grams_rpt1-grams_rpt2)
gen grams_rpt = (grams_rpt1 + grams_rpt2)/2
replace grams_rpt1 = grams_rpt if e2<=round(.02,.001) & e2>0
replace grams_rpt2 = grams_rpt if e2<=round(.02,.001) & e2>0
drop grams_rpt

drop e
gen e = (grams_rpt2-grams_rpt1)/grams_rpt1
*table drug year if e==-1 & missing(e)==0
table drug quarter if e>.07 & missing(e)==0
table drug quarter if e>-1 & e<-0.07 & missing(e)==0


** Other notes: sufentanil is just due to rounding
** Hydrocodone in 2012 is actually problematic

*br state quarter drug grams_rpt1 grams_rpt2  if e>1 & missing(e)==0
*br state quarter drug grams_rpt1 grams_rpt2  if e>-1 & e<-0.4 & missing(e)==0


twoway connected grams_rpt* quarter if state=="NEVADA" & drug=="HYDROCODONE"
twoway connected grams_rpt* quarter if state=="NEW YORK" & drug=="HYDROCODONE"
twoway connected grams_rpt* quarter if state=="OKLAHOMA" & drug=="HYDROCODONE"
twoway connected grams_rpt* quarter if state=="ALABAMA" & drug=="HYDROCODONE"

** report 2 seems to be wrong

replace grams_rpt2=grams_rpt1 if drug=="HYDROCODONE" & year==2012
drop e2 e

/*
preserve
	keep if drug=="OXYCODONE"
	replace category="Oxycodone"
	tempfile oxy
	save `oxy'
restore

preserve
	keep if drug=="HYDROCODONE"
	replace category="Hydrocodone"
	tempfile hyd
	save `hyd'
restore

append using `oxy'
append using `hyd'

collapse (sum) grams_adj_rpt2, by(category STATE quarter)*/
* SUMMARY: 2012 state data is wrong; use zip3 data

gen grams_adj_rpt1 = grams_rpt1*conversion_factor if missing(conversion_factor)==0
replace grams_adj_rpt1 = grams_rpt1*mme if missing(mme)==0

gen grams_adj_rpt2 = grams_rpt2*conversion_factor if missing(conversion_factor)==0
replace grams_adj_rpt2 = grams_rpt2*mme if missing(mme)==0

drop grams_rpt2 grams_adj_rpt2

order category drug state quarter year grams_rpt1 grams_adj_rpt1 
drop mme conversion_factor


**********************************************************************************************************

* 3. Merge in population Controls, now with 2017 update

*use `rpt1', clear
merge m:1 state year using "/Users/angelakilby/Dropbox/Research/Data/Population/data/populations_bridged_state_year_2017update.dta", keepusing(population_bridged perc_*)
drop if _merge==2 
drop _merge

*drop population_compressed
*merge m:1 state year using "/Users/angelakilby/Dropbox/Research/Data/Population/data/populations_census_state_year.dta", keepusing(population_census_estimate)
*drop if _merge==2
*drop _merge
*drop population
*gen population = population_bridged if year<2010
*replace population = population_census_estimate if year>=2010
*drop population_*

*gen grams_adj_pc = 10000*grams_adj/population

order population_bridged perc_*, last
order state year quarter

**********************************************************************************************************

* 4. Merge in unemployment controls

merge m:1 state year using "/Users/angelakilby/Dropbox/Research/Data/Unemployment/data/unemployment_state_year.dta",  assert(2 3) keepusing(state unemployed_rat year)
drop if _merge==2
drop _merge
sort category drug state quarter 

**********************************************************************************************************

ren population_bridged population

save `savepath', replace



**********************************************************************************************************
* Old data
**********************************************************************************************************
use "/Users/angelakilby/Dropbox/Research/Data/reducedform_selected_backup/arcos/output/dta/arcos_state_quarter_new.dta", clear

ren drug_name drug

	gen mme = . 
	replace mme = 75 if drug=="FENTANYL BASE"
	replace mme = 4 if drug=="HYDROMORPHONE"
	replace mme = 0.333333333 if drug=="MEPERIDINE (PETHIDINE)"
	replace mme = 1 if drug=="MORPHINE"

	replace mme = 40 if drug=="BUPRENORPHINE"
	replace mme = 0.1 if drug=="CODEINE"
	replace mme = 1 if drug=="HYDROCODONE"
	replace mme = 1.5 if drug=="OXYCODONE" 
	replace mme = 7.5 if drug=="METHADONE"

	gen grams_adj = mme*grams_master_3
	
keep quarter year state drug population perc_over60 unemployment_rate grams_adj
ren perc_over60 perc_61_max 
ren unemployment_rate unemployed_rat
	
save `savepath_olddata', replace

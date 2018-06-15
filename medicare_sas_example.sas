/* SAS Coding Example for Medicare Data */
/* Author: Kwame Bernard */
/* Date: March 21, 2017 */

/*** PROGRAM DESCRIPTION ***/
/* 	This program reads in two data sets regarding readmission and deaths from
	Medicare and FIPS data, merges the two data sets, and displays summary statistics
	regarding Acute Myocardial Infarction (AMI) 30-Day Mortality Rate by state for
	Arizona, California, Ohio, Maine, New Jersey, and Washington. It also provides the
	minimum rate of readmission after discharge from the hospital for Alameda, Los Angeles,
	Orange, and Riverside counties in California */
	
/*** RAW DATA FILE LOCATIONS ***/
/*	Mediocare readmission and death = https://data.medicare.gov/api/views/ynj2-r877/rows.csv?accessType=DOWNLOAD
	FIPS = https://www.census.gov/2010census/xls/fips_codes_website.xls

/*** CLEAN WORKING DIRECTORY ***/
/* This procedure cleans up the working library to make sure old files are not erroneously used */
proc datasets lib=work nolist kill; quit; run;

/*** SET MEDICAID AND FIPS FILE LOCATION ***/
filename f_fips '/folders/myfolders/fips_codes_website.xls';
filename f_med '/folders/myfolders/Readmissions_and_Deaths_-_Hospital.csv';

/*** IMPORT MEDICAID AND FIPS DATA TO TABLES ***/
proc import datafile=f_fips
	dbms=XLS
	out=work.fips;
	guessingrows=42000;
	getnames=YES;
run;

proc import datafile=f_med
	dbms=CSV
	out=WORK.med;
	guessingrows=68000;
	getnames=YES;
run;

/*** SUBSET DATA SETS ***/
proc sql;
	create table work.med as
	select * from work.med
	where state IN ('AZ','CA','OH','ME','NJ');
quit;

proc sql;
	create table work.fips as
	select * from work.fips
	where entity_description like 'County'
		and state_abbreviation IN ('AZ','CA','OH','ME','NJ');
quit;

/*** MEREGE DATA SETS ON COUNTY NAME AND ADD MISSING FIPS DATA ***/
/*	Note: (state_abbreviation,gu_name) in work.fips is the primary key and 
	(state, county_name) is the foreign key for work.med */
proc sql;
	create table work.med as
	select med.*, fips.county_fips_code from med left join fips
	on lower(med.county_name) = lower(fips.gu_name)
	and med.state = fips.state_abbreviation;
quit;
/* 	Manually add the county fips code for San Francisco since the record is missing
	from the fips data file and recast Score to numeric */
data work.med;
	set work.med;
	if 	county_name = 'SAN FRANCISCO' then county_fips_code = '075';
	temp = input(Score, 8.);
   	drop Score;
   	rename temp=Score;
run;

/*** AMI 30-DAY MORTALITY RATE ***/
/* Create a sorted subset of the Medicare data for summary statistics */
proc sort data = work.med out = work.med;
	by state;
run;
data ami_mort;
	set work.med;
	if measure_name ~= 'Acute Myocardial Infarction (AMI) 30-Day Mortality Rate' then delete;
run;
/* Obtain summary statistics table for AMI 30-Day Mortality Rate classed by the states mentioned above */
proc means noprint data = ami_mort;
	var score;
	class state;
	output out = ami_mort mean=mean std=stdev;
run;
data ami_mort;
	set ami_mort;
	drop _freq_ _type_;
	if state = "" then delete;
run;

/*** MINIMUM RATE OF READMISSION AFTER DISCHARGE FROM HOSPITAL FOR SUBSET OF CA COUNTIES ***/
proc sql;
	create table readm as
	select distinct county_name, county_fips_code, hospital_name, min(score) as minScore
	from work.med
	group by county_name, hospital_name;
	where county_name in ('Alameda', 'Los Angeles', 'Orange', 'Riverside')
		and state = 'CA';
quit;

/*** OUTPUT RELEVANT DATASETS ***/
proc export data=ami_mort
outfile="/folders/myfolders/ami_mort.csv";
run;
proc export data=readm
outfile="/folders/myfolders/readmission.csv";
run;

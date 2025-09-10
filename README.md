# scrapper_scripts

## Filenames format

Save new publications to *.pdf file with the following naming conventions:

YYYY-MM-DD – date of publication

Name of the report - Use the report title, replace all spaces and special characters (; : ' "{} ^ % ~# | <> \ []) in the title with "_" (number of symbols in the file name could be limited to 250)..

For example: “April 2025 euro area bank lending survey” publication should be saved with the name: “2025-04-15_April_2025_euro_area_bank_lending_survey.pdf”

If two reports with the same name and publication date are published, but the reports are different (and/or have different links), add "_At1" to the end of the name of one of these reports (for example, 2023-06-21_Nomination_hearing_At1.pdf)


## Metadata Mapping: Web Scraping to JSON



1	
dataset_name

-

System

hard-coded: Central Bank EUR

-



"dataset_name": "Central Bank EUR"
 

2	
dataset_code

-

System

hard-coded: CB_EUR_ECB

-



"dataset_code": "CB_EUR_ECB"
3	
source_uri

Direct URL to PDF

Website

Save this link as is



https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/2025/html/ecb.is250724~a66e730494.en.html


"source_uri": "https://www.ecb.europa.eu/press/press_conference/monetary-policy-statement/2025/html/ecb.is250724~a66e730494.en.html"
4	
created_at

-

System

Timestamp when Data File was loaded to the system

ISO-8601 -formatted value 
(YYYY-MM-DDTHH:MM:SS+HH:MM)

New York Time Zone by default

-



"created_at": "2025-09-01T19:15:20-04:00"
5	
creator

-

Website

Speaker provided in the website’s table for each published report

Copy value as is

If not provided, leave this field empty



Christine Lagarde, Luis de Guindos


"creator": "Christine Lagarde, Luis de Guindos"
6	
publisher

-

System

hard-coded: European Central Bank

-



"publisher": "European Central Bank"
7	
publication_date

Publication Date

Website

Publication date provided in the website’s table for each published report

Copy value as is

If not provided, leave this field empty

if date provided without time, default time to 09:00:00 NY TimeZone



24 July 2025


"publication_date": "2025-07-24T09:00:00-04:00"
8	
publication_title

Publication Title

Website

Publication title provided in the website’s table for each published report

Copy value as is - full name of the publication’s title according to source 



Monetary policy statement


"publication_title": "Monetary policy statement"
9	
ingest_source

-

System

hard-coded: CB_EUR_ECB_LDR

-



"ingest_source": "CB_EUR_ECB_LDR"
10	
custom_attributes

-

System

this object should include 2 nested objects:

Category

Language

Category values: see mapping on 
Metadata Mapping: Central Bank European | Raw Metadata File (website)
 

Language value is hard-coded: English

-



"custom_attributes": {
    "category": "Press release",
    "language": "English"
  }
11	
raw_attributes

-

-

Leave this object empty

-



"raw_attributes": {}
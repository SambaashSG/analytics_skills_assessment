# assignment 01
This script sets up a sample dataset using real OmniCom data complying with company data privacy policy.

### purpose
This purpose of the assignment is to assess the candidate’s ability to:

1. Clean-up data not in standard format
2. Process data into standard summary statistics

### specifications
and to that end, this dataset should have the following features
1. requires transformation steps such as a string json in a single table field
2. rows too large to manually manipulate the dataset but not so large to have slow processing 1-100k
3. imperfections, ex:
       rows with blank fields
       duplicates rows
       dates as string
       extra invalid characters
       data gaps
       
### Deployment

To prepare the dataset, 

1. clone a copy of analytics_skills_assessment and create a venv interpreter
2. copy the files in the folder for the assignment
       to the root folder analytics_skills_assessment\ 
3. uncomment the lines of code in the script to find the local directory
4. run the setup file from the command line

`>>python assignment_01_setup.py`

   this will create two csv files

   _marketing_sales.csv_  
   _campaigns.csv_

five. create a zip file with the 2x csv files + the INSTRUCTIONS.md file
   and send it to the person being evaluated.

### Dataset preparation

1. query client_server.SPPEProcessAudit on foreign key from table .SPPEProcessState
    where record is created after Jan 1 2020
   filter for submissions from form id: 1413

   _remarks:_    
   
   _2021-11-04 14:45 37,935 records_

2. manually inspect data to identify columns with potential confidentiality risks
     or redundant columns

   _remarks:_ 
   
   _2021-11-04 14:48 no confidential columns found_
   
   _2021-11-04 14:58 listed 23x redundant fields_

3. unpack data1 fields and inspect for confidentiality and redundancy

   _remarks:_
   
   _2021-11-04 15:13 data1 unpacked, fields merged and exported to csv for inspection_
   
   _2021-11-04 15:18 identified 13x fields to remove_

4. create anonymized unique userid field from email address field

   _remarks:_
   
   _2021-11-04 15:27 email address anonymized with new field VisitorId_

5. create simulated campaign spending

  add pattern to data make dummy table campaign act amount spent
   divide the campaigns into two sets, one before and one after July and give different
    average cost per lead ($4 and $8 for early and late respectively) and add some noise

   _remarks:_
   
   _2021-11-04 17:05 created simulated campaign spending stored into table 'campaign'_

6. corrupt the dataset

    6.1 delete all records for March 2021
    
    6.2 replace some VisitorIds with 0
    
    6.3 campaign id add invalid characters spaces
    
    6.4 add '' in create date and last modified field

   _remarks:_
   
   _2021-11-04 15:34 listed corruption process steps_
   
   _2021-11-04 17:14 deleted all records for March_
   
   _2021-11-05 15:40 for 5x rows set VisitorId to 0_
   
   _2021-11-05 16:00 for 4000x rows added ' ' space to campaign id_
   
   _2021-11-05 16:15 for 500x rows replaced create and modified date with ""_

7. repack and export to csv

   _remarks:_
   
   _2021-11-05 17:39 repacked and exported to 'marketing_sales.csv'_
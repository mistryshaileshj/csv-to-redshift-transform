# AWS Glue ETL Script: Customer Data Transformation

This project demonstrates an AWS Glue ETL script that:
- Reads customer data from an S3 bucket (CSV format)
- Transforms the data by:
  - Concatenating first and last names
  - Converting names to uppercase
  - Extracting month and year from subscription dates
  - Split column value
  - Formatting date
  - Renaming columns
- Writes the transformed output to Redshift table using spark dataframes write method

## Technologies
- AWS Glue (PySpark)
- AWS S3
- Python (Boto3)

## Sample Input
Index,Customer Id,First Name,Last Name,Company,City,Country,Phone 1,Phone 2,Email,Subscription Date,Website
1,EB54EF1154C3A78,Heather,Callahan,Mosley-David,Lake Jeffborough,Norway,043-797-5229,915.112.1727,urangel@espinoza-francis.net,26-08-2020,http://www.escobar.org/
2,10dAcafEBbA5FcA,Kristina,Ferrell,"Horn, Shepard and Watson",Aaronville,Andorra,932-062-1802,(209)172-7124x3651,xreese@hall-donovan.com,27-04-2020,https://tyler-pugh.info/
3,67DAB15Ebe4BE4a,Briana,Andersen,Irwin-Oneal,East Jordan,Nepal,8352752061,(567)135-1918,haleybraun@blevins-sexton.com,22-03-2022,https://www.mack-bell.net/
4,6d350C5E5eDB4EE,Patty,Ponce,Richardson Group,East Kristintown,Northern Mariana Islands,302.398.3833,196-189-7767x770,hohailey@anthony.com,02-07-2020,https://delacruz-freeman.org/
5,5820deAdCF23EFe,Kathleen,Mccormick,Carson-Burch,Andresmouth,Macao,001-184-153-9683x1497,552.051.2979x342,alvaradojesse@rangel-shields.com,17-01-2021,https://welch.info/

## Output Example
Sample output is a transformed dataset. Data read from csv file is written to redshift table with selected columns along with transformations.

## How to Run
This script is designed to run inside AWS Glue, but can be adapted to run locally with a Spark environment.

## Author
Shailesh Mistry – [LinkedIn](https://www.linkedin.com/in/shailesh-mistry-a346659)



# Data Processing and SQL Automation Project


## Overview
This project is designed to automate the process of loading, cleaning, and updating data in an SQL Server database using Python. The goal is to demonstrate automation through Windows Task Scheduler, Python logging, exception handling, and SQL table updates.

The project consists of:

A Python script that processes the data.
A logging mechanism for tracking errors and information.
A test suite using unittest to ensure that key components function correctly.

## Project Details

Main Features:
Data Loading: The script loads data from a CSV file.
SQL Database Interaction: It connects to an SQL Server, fetches data, and updates the existing SQL table.
Data Cleaning: The script cleans the data by removing unnecessary columns, renaming columns, and eliminating duplicates.
Error Logging: Any errors encountered during execution are logged into a file (data_processing.log).
Automation: The script is designed to be scheduled using Windows Task Scheduler for periodic execution.
Unit Testing: A separate test script is provided to test key components using unittest and mock objects.


## Structure of the Project
main_script.py: The main Python script that performs the data processing and SQL updates.
test_script.py: A unit test script that ensures the correctness of the key operations.
data_processing.log: A log file for capturing errors and informational messages during script execution.
Diwali.csv: A sample dataset used in the project (replace with your own dataset if needed).


## CSV File Example
The CSV file used in this project contains the following columns:

User_ID	Cust_name	Product_ID	Gender	Age Group	Age	Marital_Status	State	Zone	Occupation	Product_Category	Orders	Amount	Status	unnamed1
1002903	Sanskriti	P00125942	F	26-35	28	0	Maharashtra	Western	Healthcare	Auto	1	23952.0		
1000732	Kartik	P00110942	F	26-35	35	1	Andhra Pradesh	Southern	Govt	Auto	3	23934.0		
1001990	Bindu	P00118542	F	26-35	35	1	Uttar Pradesh	Central	Automobile	Auto	3	23924.0		
1001425	Sudevi	P00237842	M	0-17	16	0	Karnataka	Southern	Construction	Auto	2	23912.0		
1000588	Joni	P00057942	M	26-35	28	1	Gujarat	Western	Food Processing	Auto	2	23877.0	

## Exception Handling & Logging

All exceptions during the process are handled and logged to the data_processing.log file.
Detailed logging helps in tracking the execution and identifying any issues that might occur during data processing.
Project Dependencies

### Packages used in the project:

pandas

pyodbc

sqlalchemy

unittest

logging


## Conclusion
This project is aimed at providing a flow where data processing and SQL database interactions are automated and reliable. It showcases how to handle real-world issues such as logging, error handling, data cleaning, and automated testing

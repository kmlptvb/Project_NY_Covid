# Project_NY_Covid
collecting covid test info for each county
# Description
Problem Statement: Imagine you are part of a data team that wants to bring in daily data for COVID-19 test occurring in New York state for analysis. Your team has to design a daily workflow that would run at 9:00 AM and ingest the data into the system. 
# Implementation option:
Airflow to create a daily scheduled dag a. Utilize docker to run the Airflow and Postgres database locally b. There should be one dag containing all tasks needed to perform the end to end ETL process c. Dynamic concurrent task creation and execution in Airflow for each county based on number of counties available in the response

![image](https://user-images.githubusercontent.com/84336868/118532435-fa35f080-b714-11eb-87ba-b22c63fbb315.png)
This workflow contains four steps:
* Start -> This step indicates the starting of the workflow with simple print statement.

* get_couny_wise_data      -> This task parses the information from the URL. Once the data is collected the data will be parsed and deletes the unwanted data. Then it will create a python dictory with country specific data. As country name being the column and country specific data being the value.

*  push_countywisedata_to_countytable      -> This task will create a country specific table for each country dynamically and updates the table with new live data.
* end -> This step indicates the starting of the workflow with simple print statement.


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from sqlalchemy import create_engine
from datetime import datetime, date
import urllib.request, json
import pandas as pd

def _get_county_wise_data():
    # get the data from url.
    url_data = urllib.request.urlopen("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")

    # convert the url data to Json format.
    response = json.loads(url_data.read().decode())

    # get the data and metadata info from response.
    response_info = response["meta"]
    response_data = response["data"]

    # get the columns names.
    columns = response["meta"]["view"]["columns"]
    columns = [column["name"] for column in columns]

    # data information.
    response_data = pd.DataFrame(response_data)

    # required columns from the data as per the documentation.
    required_columns = ['County', 'Test Date', 'New Positives', 'Cumulative Number of Positives','Total Number of Tests Performed', 'Cumulative Number of Tests Performed']

    # update the response data with columns names from the metadata.
    response_data.columns = columns

    # update response data with required columns alone by deleting the unnecessary columns.
    response_data = response_data[required_columns]

    #Added the Load Date column with today's date.
    response_data["Load Date"]=date.today().strftime('%m-%d-%Y')

    #converting to date format.
    response_data["Test Date"]=pd.to_datetime(response_data["Test Date"]).dt.strftime('%m-%d-%Y')

    # Get the unique values from county column.
    counties = response_data.County.unique()

    # Create a dictonary with county specific data.
    result = {}
    for county in counties:
        temp=response_data.loc[response_data['County'] == county]
        temp=temp[temp.columns[1:]]
        result[county]=temp.to_json()
    
    return result

dag = DAG(
    dag_id="new_york_dag",
    start_date=datetime(2021,1,1),
    schedule_interval="@daily",
    catchup=False)

start = BashOperator(
    task_id="start",
    bash_command="echo 'START'",
    dag=dag)

end = BashOperator(
    task_id="end",
    bash_command="echo 'END'",
    dag=dag)

get_county_wise_data = PythonOperator(
    task_id="get_county_wise_data",
    python_callable=_get_county_wise_data,
    dag=dag)

def _push_countywisedata_to_countytable(ti):
    # Get the county data from previous step.
    result = ti.xcom_pull(task_ids='get_county_wise_data')

    # Printing county data to logs.
    counties = list(result)
    print(counties)

    # Creating postgres engine to create the county tables.
    engine = create_engine('postgresql://airflow:airflow@172.18.0.3:5432/airflow')
    
    # Updating the county table with latest data successfully.
    for i in range(len(result.keys())):
        county=counties[i]
        county_data=pd.DataFrame(json.loads(result[county]))

        # Loading the DB with new data.
        county_data.to_sql(county, con=engine, if_exists='replace')
        print("Successfully loaded the county:{} data into database.".format(county))

push_countywisedata_to_countytable = PythonOperator(
    task_id="push_countywisedata_to_countytable",
    python_callable=_push_countywisedata_to_countytable,
    dag=dag)

start.set_downstream(get_county_wise_data)
get_county_wise_data.set_downstream(push_countywisedata_to_countytable)
push_countywisedata_to_countytable.set_downstream(end)

from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

def extract_data_from_csv(csv_path):
    df = pd.read_csv(csv_path,names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type','Number of axles','Vehicle code'])
    df = df[['Rowid', 'Timestamp', 'Anonymized Vehicle number','Vehicle type']]
    df.to_csv('/home/project/airflow/dags/python_etl/staging/csv_data.csv',index=False)
    if os.path.exists('/home/project/airflow/dags/python_etl/staging/csv_data.csv'):
        print("File 'tsv_data.csv' has been successfully created.")
    else:
        print("Error: The file 'tsv_data.csv' was not created.")
def extract_data_from_tsv(tsv_path):
    df = pd.read_csv(tsv_path,names=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type','Number of axles', 'Tollplaza id', 'Tollplaza code'],sep='\t')
    df = df[['Number of axles', 'Tollplaza id','Tollplaza code']]
    df.to_csv('/home/project/airflow/dags/python_etl/staging/tsv_data.csv',index=False)
def extract_data_from_fixed_width(fixed_width_path):
    column_widths = [6, 25, 10, 4, 10, 3, 5]  # Rowid, Timestamp, Anonymized Vehicle number, Tollplaza id, Tollplaza code, Type of Payment code, Vehicle Code
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']
    
    # Read the fixed-width file using pandas' read_fwf (fixed-width formatted) function
    df = pd.read_fwf(fixed_width_path, widths=column_widths, names=column_names)
    
    # Extract the required columns: 'Type of Payment code' and 'Vehicle Code'
    extracted_data = df[['Type of Payment code', 'Vehicle Code']]
    
    # Save the extracted data to a CSV file
    extracted_data.to_csv('/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv', index=False)
def consolidate_data(csv_path, tsv_path, fixed_width_path):
    # Read the three CSV files into separate DataFrames
    csv_data = pd.read_csv(csv_path)
    tsv_data = pd.read_csv(tsv_path)
    fixed_width_data = pd.read_csv(fixed_width_path)
    
    # Concatenate the DataFrames along columns (axis=1)
    consolidated_data = pd.concat([csv_data, tsv_data,
                                   fixed_width_data], axis=1)
    
    # Reorganize columns into the required order
    final_columns = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type',
                     'Number of axles', 'Tollplaza id', 'Tollplaza code', 
                     'Type of Payment code', 'Vehicle Code']
    
    # Select the columns in the required order
    consolidated_data = consolidated_data[final_columns]
    
    # Save the consolidated data to 'extracted_data.csv'
    consolidated_data.to_csv('/home/project/airflow/dags/python_etl/staging/extracted_data.csv', index=False)
    
    print("Data has been successfully consolidated and saved to extracted_data.csv")
def transform_data(data_path):
    # Read the extracted_data.csv file
    extracted_data = pd.read_csv(data_path)
    # Transform the 'Vehicle type' column to uppercase
    extracted_data['Vehicle type'] = extracted_data['Vehicle type'].str.upper()
    
    # Save the transformed data to 'transformed_data.csv' in the 'staging' directory
    extracted_data.to_csv('/home/project/airflow/dags/python_etl/staging/transformed_data.csv', index=False)
    
    print("Data has been successfully transformed and saved to staging/transformed_data.csv")
default_args = {
    'owner': 'ana',
    'start_date': datetime.today().replace(hour=0, minute=0, second=0, microsecond=0),
    'email': 'mohamed_emad41@ymail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id='ETL_toll_data',
    default_args=default_args, 
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',
) as dag:
    first_task = BashOperator(
    task_id='download_data',
    bash_command=(
        'curl -o /home/project/airflow/dags/python_etl/staging/tolldata.tgz '
        'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
        )
    )

    second_task = BashOperator(
        task_id='unzip_data',
        bash_command=(
            'tar -xzvf /home/project/airflow/dags/python_etl/staging/tolldata.tgz -C /home/project/airflow/dags/python_etl/staging'
        )
    )
    third_task = PythonOperator(
        task_id='extract_data_from_csv',
        python_callable=extract_data_from_csv,
        op_kwargs={'csv_path': '/home/project/airflow/dags/python_etl/staging/vehicle-data.csv'},
    )
    fourth_task = PythonOperator(
        task_id='extract_data_from_tsv',
        python_callable=extract_data_from_tsv,
        op_kwargs={'tsv_path': '/home/project/airflow/dags/python_etl/staging/tollplaza-data.tsv'},
    )
    fifth_task = PythonOperator(
        task_id='extract_data_from_fixed_width',
        python_callable=extract_data_from_fixed_width,
        op_kwargs={'fixed_width_path': '/home/project/airflow/dags/python_etl/staging/payment-data.txt'},
    )
    sixth_task = PythonOperator(
        task_id='consolidate_data',
        python_callable=consolidate_data,
        op_kwargs={'csv_path': '/home/project/airflow/dags/python_etl/staging/csv_data.csv',
                   'tsv_path': '/home/project/airflow/dags/python_etl/staging/tsv_data.csv',
                   'fixed_width_path': '/home/project/airflow/dags/python_etl/staging/fixed_width_data.csv'},
    )
    seventh_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={'data_path': '/home/project/airflow/dags/python_etl/staging/extracted_data.csv'}
    )

    first_task >> second_task >> [third_task, fourth_task, fifth_task ]>> sixth_task >> seventh_task




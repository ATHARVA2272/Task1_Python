import os
import argparse
import json
import pandas as pd


def list_files_in_directory(directory):
    return [os.path.join(directory, f) for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]


def upload_csv_to_mysql(file_path, table_name, connection):
    df = pd.read_csv(file_path)
    df.fillna('default_value', inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.dropna(subset=['Date'])

    cursor = connection.cursor()
    for _, row in df.iterrows():
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(row))
        query = f"INSERT INTO {table_name} (Sr_No,Date,Startup_Name,Industry_Vertical,SubVertical,City,Investors_Name,InvestmentType,Amount_in_USD,Remarks) VALUES ({placeholders})"
        cursor.execute(query, tuple(row))
    connection.commit()
    print(f"Uploaded {file_path} to {table_name}")
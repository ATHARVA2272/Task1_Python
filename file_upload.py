import os
import argparse
import json
import pandas as pd
import mysql.connector
import file_directory
import connection

def parse_arguments():
    parser = argparse.ArgumentParser(description="File Upload Utility")
    parser.add_argument('--source_dir', required=True, help="Source directory containing files to upload")
    parser.add_argument('--mysql_details', required=True, help="Path to JSON file containing MySQL connection details")
    parser.add_argument('--destination_table', required=True, help="Name of the MySQL table to upload data to")
    return parser.parse_args()

def load_mysql_details(json_path):
    with open(json_path, 'r') as file:
        return json.load(file)


def main():
    args = parse_arguments()
    mysql_details = load_mysql_details(args.mysql_details)
    connection1 = connection.create_mysql_connection(mysql_details)

    files = file_directory.list_files_in_directory(args.source_dir)
    for file_path in files:
        if file_path.endswith('.csv'):
            file_directory.upload_csv_to_mysql(file_path, args.destination_table, connection1)
        else:
            print(f"Skipping non-CSV file: {file_path}")

    connection1.close()
    print("MySQL connection closed.")

if __name__ == "__main__":
    main()


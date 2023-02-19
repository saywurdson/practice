import duckdb
import argparse

# Create argument parser to allow user to choose overwrite or append
parser = argparse.ArgumentParser(description='Create Synthea tables.')
parser.add_argument('--mode', choices=['overwrite', 'append'], default='overwrite',
                    help='Mode to use when creating tables (overwrite or append)')

# Parse arguments
args = parser.parse_args()

# create the connection
con = duckdb.connect('/workspaces/practice/omop.db')
print("Database connection established.")

# create schema
con.execute('create schema if not exists synthea')

# create tables
tables = [
    'care_site', 'condition_occurrence', 'death', 'drug_exposure', 'location',
    'measurement', 'observation_period', 'observation', 'person', 'personmap', 'procedure_occurrence', 'provider', 'visit_occurrence', 'visitmap'
]

for table in tables:
    if args.mode == 'overwrite':
        print(f'Creating table: {table}...')
        con.execute(f'create or replace table synthea.{table} as select * from read_csv_auto("/workspaces/practice/synthea/output/csv/BASE_OUTPUT_DIRECTORY/{table}.csv", ignore_errors=1, all_varchar=True)')
        print(f'Table {table} created.')
    elif args.mode == 'append':
        print(f'Appending to table: {table}...')
        con.execute(f'insert into synthea.{table} select * from read_csv_auto("/workspaces/practice/synthea/output/csv/BASE_OUTPUT_DIRECTORY/{table}.csv", ignore_errors=1, all_varchar=True)')
        print(f'Table {table} appended.')

# close the connection
con.close()
print("Database connection closed.")
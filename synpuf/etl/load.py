import duckdb, sys, os, dotenv, csv, re
import pandas as pd

dotenv.load_dotenv(".env")

BASE_OUTPUT_DIRECTORY = os.environ['BASE_OUTPUT_DIRECTORY']

# create the connection
con = duckdb.connect('/workspaces/dec-etl-project/data/omop.db')
print("Database connection established.")

# create schema
con.execute('create schema if not exists synpuf')

# create metadata table
con.execute('create table if not exists synpuf.logs (timestamp timestamp, message varchar)')

# create tables
tables = [
    'care_site', 'condition_occurrence', 'death', 'device_cost',
    'device_exposure', 'drug_cost', 'drug_exposure', 'location',
    'measurement_occurrence', 'observation_period', 'observation',
    'payer_plan_period', 'person', 'procedure_cost', 'procedure_occurrence',
    'provider', 'specimen', 'visit_cost', 'visit_occurrence'
]

arg = sys.argv[1]
while arg != 'overwrite' and arg != 'append':
    print('Invalid argument. Please enter either "overwrite" or "append".')
    arg = input()

for table in tables:
    for i in range(1, 21):
        csv_file = f'{table}_{i}.csv'
        file_path = f'/workspaces/dec-etl-project/data/BASE_OUTPUT_DIRECTORY/{csv_file}'
        if os.path.exists(file_path) and file_path.endswith('.csv'):
            print(f'Updating table: {table}...')
            if arg == 'overwrite':
                con.execute(f'create or replace table synpuf.{table} as select * from read_csv_auto("{file_path}", ignore_errors=1, all_varchar=True)')
            elif arg == 'append':
                con.execute(f'insert into synpuf.{table} select * from read_csv_auto("{file_path}", ignore_errors=1, all_varchar=True)')
            print(f'Table {table} updated.')
        else:
            print(f'File {csv_file} not found or is not a csv file.')

def extract_etl_stats(BASE_OUTPUT_DIRECTORY):
    print('processing log files...')
    pattern = re.compile(r'\[(.*?)\](.*)')
    etl_stats_df = pd.DataFrame(columns=['Timestamp', 'Message'])
    
    for i in range(1, 21):
        log_file_path = os.path.join(BASE_OUTPUT_DIRECTORY, f'etl_stats_{i}.txt')
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as f:
                for line in f:
                    match = pattern.match(line)
                    if match:
                        etl_stats_df = pd.concat([etl_stats_df, pd.DataFrame({'Timestamp': [match.group(1)], 'Message': [match.group(2)]})], ignore_index=True)
    return etl_stats_df

# convert etl_stats_df to dataframe
etl_stats_df = extract_etl_stats(BASE_OUTPUT_DIRECTORY)

# use con.execute to add etl_stats_df to the metadata table
print('Building logs table...')
con.execute('insert into synpuf.logs select * from etl_stats_df')

# close the connection
con.close()
print("Database connection closed.")
import duckdb
import pandas as pd
import logging
import argparse

def update_metathesaurus(db_path, metathesaurus_path):

    # create a logger
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # create a connection to the database
    con = duckdb.connect(db_path)

    # create a schema named METATHESAURUS
    con.execute('create schema if not exists METATHESAURUS')

    # use pandas to read the csv file
    logging.info('Creating METATHESAURUS.MRCONSO table')
    MRCONSO = pd.read_csv(f'{metathesaurus_path}/MRCONSO.RRF', sep='|', header=None, names=['CUI', 'LAT', 'TS', 'LUI', 'STT', 'SUI', 'ISPREF', 'AUI', 'SAUI', 'SCUI', 'SDUI', 'SAB', 'TTY', 'CODE', 'STR', 'SRL', 'SUPPRESS', 'CVF', 'RESERVE'], dtype={"SCUI": str, "SDUI": str, "CODE": str})

    # drop the RESERVE column
    MRCONSO = MRCONSO.drop(columns=['RESERVE'])

    con.execute('create or replace table METATHESAURUS.MRCONSO as select * from MRCONSO')
    logging.info('METATHESAURUS.MRCONSO table created')

    # close the connection
    con.close()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Update the metathesaurus data in a DuckDB database.')
    parser.add_argument('--db_path', type=str, help='Path to the DuckDB database file', required=True)
    parser.add_argument('--metathesaurus_path', type=str, help='Path to the metathesaurus data directory', required=True)
    args = parser.parse_args()

    db_path = args.db_path
    metathesaurus_path = args.metathesaurus_path

    update_metathesaurus(db_path, metathesaurus_path)



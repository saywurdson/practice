import duckdb
import pandas as pd
import csv
import logging
import argparse

def update_rxnorm(db_path, rxnorm_path):

    # create a logger
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    # create a connection to the database
    con = duckdb.connect(db_path)
    
    # create a schema named RXNORM
    con.execute('create schema if not exists RXNORM')


    # create a table named RXNORM.RXNATOMARCHIVE and add to the database
    logging.info('Creating RXNORM.RXATOMARCHIVE table')
    con.execute('''
        create or replace table RXNORM.RXATOMARCHIVE(
            RXAUI             varchar(8) NOT NULL,
            AUI               varchar(10),
            STR               varchar(4000) NOT NULL,
            ARCHIVE_TIMESTAMP varchar(280) NOT NULL,
            CREATED_TIMESTAMP varchar(280) NOT NULL,
            UPDATED_TIMESTAMP varchar(280) NOT NULL,
            CODE              varchar(50),
            IS_BRAND          varchar(1),
            LAT               varchar(3),
            LAST_RELEASED     varchar(30),
            SAUI              varchar(50),
            VSAB              varchar(40),
            RXCUI             varchar(8),
            SAB               varchar(20),
            TTY               varchar(20),
            MERGED_TO_RXCUI   varchar(8)
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXATOMARCHIVE from '{rxnorm_path}/RXNATOMARCHIVE.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXATOMARCHIVE table created')

    # create a table named RXNORM.RXNCONSO and add to the database
    logging.info('Creating RXNORM.RXNCONSO table')
    con.execute('''
        create or replace table RXNORM.RXNCONSO(
            RXCUI             varchar(8) NOT NULL,
            LAT               varchar (3) NOT NULL,
            TS                varchar (1),
            LUI               varchar(8),
            STT               varchar (3),
            SUI               varchar (8),
            ISPREF            varchar (1),
            RXAUI             varchar(8) NOT NULL,
            SAUI              varchar (50),
            SCUI              varchar (50),
            SDUI              varchar (50),
            SAB               varchar (20) NOT NULL,
            TTY               varchar (20) NOT NULL,
            CODE              varchar (50) NOT NULL,
            STR               varchar (3000) NOT NULL,
            SRL               varchar (10),
            SUPPRESS          varchar (1),
            CVF               varchar(50)
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNCONSO from '{rxnorm_path}/RXNCONSO.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNCONSO table created')
    

    # create a table named RXNORM.RXNREL and add to the database
    logging.info('Creating RXNORM.RXNREL table')
    rxnel_cols = ['RXCUI1', 'RXAUI1', 'STYPE1', 'REL', 'RXCUI2', 'RXAUI2', 'STYPE2', 'RELA', 'RUI', 'SRUI', 'SAB', 'SL', 'RG', 'DIR', 'SUPPRESS', 'CVF', 'extra']

    with open(f'{rxnorm_path}/RXNREL.RRF', 'r') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)
        data = [row for row in reader]

    # convert data to a pandas dataframe named RXNREL with the columns from header
    RXNREL = pd.DataFrame(data)

    # set the column names of RXNREL to cols
    RXNREL.columns = rxnel_cols

    # drop the extra column
    RXNREL.drop('extra', axis=1, inplace=True)

    con.execute('create or replace table RXNORM.RXNREL as select * from RXNREL')
    logging.info('RXNORM.RXNREL table created')


    # create a table named RXNORM.RXNSAB and add to the database
    logging.info('Creating RXNORM.RXNSAB table')
    con.execute('''
        create or replace table RXNORM.RXNSAB(
            VCUI           varchar (8),
            RCUI           varchar (8),
            VSAB           varchar (40),
            RSAB           varchar (20) NOT NULL,
            SON            varchar (3000),
            SF             varchar (20),
            SVER           varchar (20),
            VSTART         varchar (10),
            VEND           varchar (10),
            IMETA          varchar (10),
            RMETA          varchar (10),
            SLC            varchar (1000),
            SCC            varchar (1000),
            SRL            integer,
            TFR            integer,
            CFR            integer,
            CXTY           varchar (50),
            TTYL           varchar (300),
            ATNL           varchar (1000),
            LAT            varchar (3),
            CENC           varchar (20),
            CURVER         varchar (1),
            SABIN          varchar (1),
            SSN            varchar (3000),
            SCIT           varchar (4000)
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNSAB from '{rxnorm_path}/RXNSAB.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNSAB table created')


    # create a table named RXNORM.RXNSAT and add to the database
    logging.info('Creating RXNORM.RXNSAT table')
    cols = ['RXCUI', 'LUI', 'SUI', 'RXAUI', 'STYPE', 'CODE', 'ATUI', 'SATUI', 'ATN', 'SAB', 'ATV', 'SUPPRESS', 'CVF', 'extra']

    with open(f'{rxnorm_path}/RXNSAT.RRF', 'r') as f:
        reader = csv.reader(f, delimiter='|')
        header = next(reader)
        data = [row for row in reader]
        
    # convert data to a pandas dataframe named RXNSAT with the columns from header
    RXNSAT = pd.DataFrame(data)

    # set the column names of RXNSAT to cols
    RXNSAT.columns = cols

    # drop the extra column
    RXNSAT.drop('extra', axis=1, inplace=True)

    con.execute('create or replace table RXNORM.RXNSAT as select * from RXNSAT')
    logging.info('RXNORM.RXNSAT table created')


    # create a table named RXNORM.RXNSTY and add to the database
    logging.info('Creating RXNORM.RXNSTY table')
    con.execute('''
        create or replace table RXNORM.RXNSTY(
            RXCUI          varchar(8) NOT NULL,
            TUI            varchar (4),
            STN            varchar (100),
            STY            varchar (50),
            ATUI           varchar (11),
            CVF            varchar (50)
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNSTY from '{rxnorm_path}/RXNSTY.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNSTY table created')


    # create a table named RXNORM.RXNDOC and add to the database
    logging.info('Creating RXNORM.RXNDOC table')
    con.execute('''
        create or replace table RXNORM.RXNDOC(
            DOCKEY      varchar(50) NOT NULL,
            VALUE       varchar(1000),
            TYPE        varchar(50) NOT NULL,
            EXPL        varchar(1000)
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNDOC from '{rxnorm_path}/RXNDOC.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNDOC table created')


    # create a table named RXNORM.RXNCUICHANGES and add to the database
    logging.info('Creating RXNORM.RXNCUICHANGES table')
    con.execute('''
        create or replace table RXNORM.RXNCUICHANGES(
            RXAUI         varchar(8),
            CODE          varchar(50),
            SAB           varchar(20),
            TTY           varchar(20),
            STR           varchar(3000),
            OLD_RXCUI     varchar(8) NOT NULL,
            NEW_RXCUI     varchar(8) NOT NULL
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNCUICHANGES from '{rxnorm_path}/RXNCUICHANGES.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNCUICHANGES table created')


    # create a table named RXNORM.RXNCUI and add to the database
    logging.info('Creating RXNORM.RXNCUI table')
    con.execute('''
        create or replace table RXNORM.RXNCUI(
            cui1 VARCHAR(8),
            ver_start VARCHAR(40),
            ver_end   VARCHAR(40),
            cardinality VARCHAR(8),
            cui2       VARCHAR(8) 
        )
    ''')

    con.execute(f'''
        copy RXNORM.RXNCUI from '{rxnorm_path}/RXNCUI.RRF'(
            delimiter '|',
            header 'false'
        )
    ''')
    logging.info('RXNORM.RXNCUI table created')
    logging.info('RXNORM tables created')

    # close the connection
    con.close()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Update RxNorm data in a DuckDB database')
    parser.add_argument('--db_path', type=str, help='Path to DuckDB database file', required=True)
    parser.add_argument('--rxnorm_path', type=str, help='Path to RxNorm data directory', required=True)
    args = parser.parse_args()
    
    db_path = args.db_path
    rxnorm_path = args.rxnorm_path
    
    update_rxnorm(db_path, rxnorm_path)
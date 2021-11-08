""" this module is used to explore the sql databases
 used to generate Sambaash AWS QuickSight analytics
"""

# dependencies
import pandas as pd
import numpy as np
import datetime as dt
import json
import copy
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine.reflection import Inspector

#---------------------------------------------------------------------------------------
# constants
#---------------------------------------------------------------------------------------
DATABASES = {
    'production': {
        'database': 'analytics_qs',
        'host': 'db-sambaashplatform-cluster-1.cluster-ro-cj4nqileqmph.ap-southeast-1.rds.amazonaws.com',
        'login_url': 'mysql+pymysql://{user}:{pw}@db-sambaashplatform-cluster-1.cluster-cj4nqileqmph.ap-southeast-1.rds.amazonaws.com/{db}',
        'engine': None,
        'con': None
    },
    'omnicom': {
        'database': 'lithan',
        'host': 'db-sambaashplatform-cluster-1.cluster-ro-cj4nqileqmph.ap-southeast-1.rds.amazonaws.com',
        'login_url': 'mysql+pymysql://{user}:{pw}@db-sambaashplatform-cluster-1.cluster-ro-cj4nqileqmph.ap-southeast-1.rds.amazonaws.com/{db}',
        'engine': None,
        'con': None
    },
    'staging': {
        'database': 'AnalyticsTesting',
        "login_url": "mysql+pymysql://{user}:{pw}@db-sambaashplatform-cluster-1.cluster-cj4nqileqmph.ap-southeast-1.rds.amazonaws.com/{db}",
        'engine': None,
        'con': None
    }
}

#---------------------------------------------------------------------------------------
# dynamic varibles
#---------------------------------------------------------------------------------------
inspector = None
table_names = []
LOGIN_CONFIG = None

#---------------------------------------------------------------------------------------
# setup
#---------------------------------------------------------------------------------------


def load_config():
    global LOGIN_CONFIG
    LOGIN_CONFIG = json.load(open('login_config.json'))


#---------------------------------------------------------------------------------------
# main
#---------------------------------------------------------------------------------------


def explore_example(srv='production'):
    """ steps in this procedure:
    1. setup connection to the mysql database using sqlalchemy
    2. import the sql tables into pandas DataFrames as a query for new records
    3. perform the table operations to create report output tables
    4. export the results to csv file
    """
    # 00 load configuration
    load_config()

    # 01 setup connection(s) to the mysql source and destination database(s) using sqlalchemy
    db_connect(srv)

    # 02 import the sql tables into pandas DataFrames as a query for new records
    #qry_tbl = pd.read_sql(sql_select_table(SCHEMA['source']['forecast']['name']),
    #    DATABASES['source']['conn'])


    # 03 perform the table operations to create the report


    # 04 export report to csv


def db_connect(srv='production'):
    """ create a connect to the SQL database
    """
    global DATABASES
    db_name = DATABASES[srv]['database']
    print('Connecting to %s on %s server' % (db_name, srv))
    DATABASES[srv]['engine'] = create_engine(
        DATABASES[srv]['login_url'].format(
            user=LOGIN_CONFIG['user'],
            pw=LOGIN_CONFIG['password'],
            db=db_name))
    DATABASES[srv]['con'] = DATABASES[srv]['engine'].connect()
    print('Connected to %s on %s server' % (db_name, srv))


def db_close(srv='production'):
    db_name = DATABASES[srv]['database']
    if DATABASES[srv]['con'] is not None:
        DATABASES[srv]['con'].close()
        print('Connection closed to %s on %s server' % (db_name, srv))


def query_from_sql_file(sql_filename, srv='production'):
    global DATABASES
    qry_file = open(sql_filename, "r")
    qry_stm = qry_file.read()
    qry_tbl = pd.read_sql(qry_stm, con=DATABASES[srv]['con'])
    qry_file.close()
    return qry_tbl
#---------------------------------------------------------------------------------------
# SQL syntax
#---------------------------------------------------------------------------------------


def sql_select_table(table_name):
    return 'SELECT * FROM ' + table_name


if __name__ == "__main__":
    explore_example()


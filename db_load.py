import os
import json
from psycopg2 import connect

## access DB info with json
def json_config(db_name):
    config_path= os.path.join('config', os.listdir(os.path.join('config'))[0])  ## directory to config.json
    with open(config_path) as f:
        config= json.load(f)
    host = config[db_name]['host']
    dbname = config[db_name]['dbname']
    user = config[db_name]['user']
    password = config[db_name]['password']
    port= config[db_name]['port']
    return host, dbname, user, password, port


## For connecting DB
def db_connection(local_db):
    tmp_host, tmp_dbname, tmp_user, tmp_password, tmp_port= json_config(local_db)
    tmp_conn= connect(host= tmp_host, dbname= tmp_dbname, user= tmp_user,
        password= tmp_password, port= tmp_port, connect_timeout= 3)
    return tmp_conn
from typing import AsyncContextManager
import snowflake.connector
from codecs import open
import argparse

User = ''
Password = ''
Account = ''
Warehouse = ''
Database = ''
Schema = ''
parser = argparse.ArgumentParser()
subparser = parser.add_subparsers(dest='command')
login = subparser.add_parser('login')

login.add_argument('-u', type=str, required=True)
login.add_argument('-p', type=str, required=True)
login.add_argument('-a', type=str, required=True)
login.add_argument('-w', type=str, required=True)
login.add_argument('-db', type=str, required=True)
login.add_argument('-s', type=str, required=True)

args = parser.parse_args()


if args.command == 'login':
    User = args.u
    Password = args.p
    Account = args.a
    Warehouse = args.w 
    Database = args.db
    Schema = args.s     

print("opening....")

cnn = snowflake.connector.connect(
    user = User,
    password = Password,
    account = Account,
    warehouse = Warehouse,
    database = Database,
    schema = Schema

)

sql_file = 'dbscripts/sql_statements.sql'

with open(sql_file, 'r' , encoding='utf-8') as f :
    for cs in cnn.execute_stream(f) :
        for rt in cs :
            print(rt)




cnn.close()


print("done..")



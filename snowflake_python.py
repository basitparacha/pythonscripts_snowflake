import snowflake.connector
from codecs import open


print("opening....")

cnn = snowflake.connector.connect(
    user = 'AbdulBasit',
    password = 'B1a2s3i4t56',
    account = 'ak67299.east-us-2.azure',
    warehouse = 'DEMO_WH',
    database = 'DEMO_DB',
    schema = 'DEMO_2'

)

sql_file = 'dbscripts/sql_statements.sql'

with open(sql_file, 'r' , encoding='utf-8') as f :
    for cs in cnn.execute_stream(f) :
        for rt in cs :
            print(rt)




cnn.close()


print("done..")



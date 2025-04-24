import os
import snowflake.connector

try:
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT']
    )
    conn.cursor().execute(f'USE DATABASE {os.environ["SNOWFLAKE_DATABASE"]}')
    conn.cursor().execute(f'USE SCHEMA {os.environ["SNOWFLAKE_SCHEMA"]}')
    conn.cursor().execute(f'USE WAREHOUSE {os.environ["SNOWFLAKE_WAREHOUSE"]}')

    with open('my_query3.sql', 'r') as file:
        sql_script = file.read()

    for stmt in sql_script.split(';'):
        if stmt.strip():
            conn.cursor().execute(stmt)

    print('SQL execution completed successfully.')

except Exception as e:
    print(f'Error occurred: {e}')

finally:
    if conn:
        conn.close()

import pandas as pd
from sqlalchemy import create_engine, text
from configparser import ConfigParser

def config(filename='d:/postgre/postgresql.ini', section='postgresql'):
    # Creating a parser
    parser = ConfigParser()
    # Read config file
    parser.read(filename)

    # Get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    # required keys check
    required_keys = ['host', 'dbname', 'user', 'password', 'port']
    for key in required_keys:
        if key not in db:
            raise Exception(f'Missing required configuration key: {key}')
    
    return db

def read_excel(file_path):
    df = pd.read_excel(file_path)
    return df

def create_table(engine, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        question_number TEXT,
        question TEXT
    )
    """
    with engine.connect() as connection:
        connection.execute(text(create_table_query))
        print(f"Table {table_name} created successfully.")

def insert_data(engine, table_name, df):
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Data inserted into {table_name} successfully.")

def main(action, file_path=None, table_name='python_questions'):
    if action == 'insert':
        if file_path:
            params = config()
            db_url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['dbname']}"
            engine = create_engine(db_url)

            df = read_excel(file_path)
            create_table(engine, table_name)
            insert_data(engine, table_name, df)
        else:
            print("Error: Please provide the file path to insert.")
    else:
        print("Invalid action. Use 'insert'.")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Insert Excel data into PostgreSQL.')
    parser.add_argument('action', choices=['insert'], help="Action to perform: 'insert'")
    parser.add_argument('--file_path', required=True, help="Path to the Excel file to insert")
    parser.add_argument('--table_name', default='python_questions', help="Name of the table to insert data into")

    args = parser.parse_args()
    main(args.action, file_path=args.file_path, table_name=args.table_name)

import pandas as pd
from sqlalchemy import create_engine
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

    return db

def export_to_excel(query, excel_filename):
    # Read connection parameters
    params = config()

    # Create connection string for SQLAlchemy
    db_url = f"postgresql+psycopg2://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"

    # Create SQLAlchemy engine
    engine = create_engine(db_url)
    
    try:
        # pandas to execute query and reading data into a DataFrame
        df = pd.read_sql_query(query, engine)
        
        # Export the DataFrame to an Excel file
        df.to_excel(excel_filename, index=False)
        print(f"Data exported successfully to {excel_filename}")
    except Exception as error:
        print(error)

if __name__ == '__main__':
    query = 'SELECT * FROM public."Cars"'  #  table name and schema 
    excel_filename = 'Cars_data.xlsx'  # Name of Excel file
    export_to_excel(query, excel_filename)

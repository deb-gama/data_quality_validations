import pandas as pd
from sqlalchemy import create_engine

def connect_postgres_engine(db_config: dict):
    """
    Create an SQLAlchemy engine to connect to PostgreSQL.
    :param db_config: Dictionary containing PostgreSQL connection details.
    :return: SQLAlchemy engine object
    """
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config.get('port', 5432)}/{db_config['database']}"
        )
        print("PostgreSQL engine created successfully!")
        return engine
    except Exception as e:
        print(f"Error creating PostgreSQL engine: {e}")
        return None


def query_to_dataframe(query: str, db_config: dict) -> pd.DataFrame:
    """
    Executes a query and returns the results as a Pandas DataFrame.
    :param query: SQL query string.
    :param db_config: Dictionary containing PostgreSQL connection details.
    :return: DataFrame with the query results.
    """
    engine = connect_postgres_engine(db_config)
    if engine:
        try:
            df = pd.read_sql(query, engine)
            print("Query executed successfully!")
            return df
        except Exception as e:
            print(f"Error executing query: {e}\n Query: {query}")
        finally:
            engine.dispose()
            print("Connection closed.")
    else:
        return pd.DataFrame()  


def create_or_append_table(df: pd.DataFrame, table_name: str, db_config: dict):
    """
    Create a table in PostgreSQL from a DataFrame, or append data if the table already exists.
    :param df: DataFrame to write to PostgreSQL.
    :param table_name: Name of the table in PostgreSQL.
    :param db_config: Dictionary containing PostgreSQL connection details.
    """
    engine = connect_postgres_engine(db_config)
    if engine:
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Data successfully written to {table_name} in PostgreSQL.")
        except Exception as e:
            print(f"Error writing DataFrame to PostgreSQL: {e}")
        finally:
            engine.dispose()
            print("Connection closed.")
    else:
        print("Failed to create PostgreSQL engine.")

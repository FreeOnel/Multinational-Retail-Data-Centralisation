import yaml
import sqlalchemy as sa
import pandas as pd

class DatabaseConnector:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        db_url = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=creds['RDS_USER'],
            password=creds['RDS_PASSWORD'],
            host=creds['RDS_HOST'],
            port=creds['RDS_PORT'],
            database=creds['RDS_DATABASE']
        )
        engine = sa.create_engine(db_url)
        return engine

    def list_db_tables(self):
        inspector = sa.inspect(self.engine)
        tables = inspector.get_table_names()
        print("Tables in the database:")
        for idx, table in enumerate(tables):
            print(f"{idx}. {table}")
        print()

    def upload_to_db(self, df: pd.DataFrame, table_name):
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
        print("Uploaded to table successfully.")

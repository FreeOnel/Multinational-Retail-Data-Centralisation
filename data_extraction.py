import tabula
import pandas as pd

class DataExtractor:
    
    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        data = pd.read_sql(query, engine)
        return data
    
    def retrieve_pdf_data(self, pdf_link):
        dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        return pd.concat(dfs, ignore_index=True)

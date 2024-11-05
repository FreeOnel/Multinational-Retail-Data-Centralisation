from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

input_yaml_file = "db_creds.yaml"
input_connector = DatabaseConnector(input_yaml_file)

input_connector.list_db_tables()

extractor = DataExtractor()
table_name = "legacy_users"
data = extractor.read_rds_table(input_connector, table_name)

# print(data.head())
print(f"Table rows: {data.shape[0]}")

cleaner = DataCleaning()
cleaned_data = cleaner.clean_user_data(data)

# print(cleaned_data.head())
print(f"Cleaned table rows: {cleaned_data.shape[0]}")

output_yaml_file = "sales_data_db_creds.yaml"
output_connector = DatabaseConnector(output_yaml_file)
output_connector.upload_to_db(cleaned_data, "dim_users")

PDF_link = 'TBC'
card_data = extractor.retrieve_pdf_data(PDF_link)
cleaned_card_data = cleaner.clean_card_data(card_data)
output_connector.upload_to_db(cleaned_card_data, "dim_card_details")
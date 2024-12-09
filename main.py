from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

def check_value(df, expected_value, label=''):
    actual_value = df.shape[0]
    assert actual_value == expected_value, f"Expected: {expected_value}, Actual: {actual_value}"
    print(f"Cleaned {label} table rows: {actual_value}")

def extract_and_clean_user_data():
    data = extractor.read_rds_table(input_connector, input_user_table_name)
    cleaned_data = cleaner.clean_user_data(data)
    check_value(cleaned_data, expected_value=15284, label='user') # Check cleaning method
    output_connector.upload_to_db(cleaned_data, output_user_table_name)

def extract_and_clean_card_data():
    card_data = extractor.retrieve_pdf_data(PDF_link)
    cleaned_card_data = cleaner.clean_card_data(card_data)
    check_value(cleaned_card_data, expected_value=15284, label='card')
    output_connector.upload_to_db(cleaned_card_data, output_card_table_name)

def extract_and_clean_store_details():
    store_count = extractor.list_number_of_stores(number_stores_endpoint, headers)
    print(f"Number of stores to retrieve: {store_count}")
    stores_df = extractor.retrieve_stores_data(store_endpoint)
    cleaned_store_data = cleaner.clean_store_data(stores_df)
    check_value(cleaned_store_data, expected_value=441, label='store') 
    output_connector.upload_to_db(cleaned_store_data, output_store_table_name)

def extract_and_clean_product_details():
    products_df = extractor.extract_from_s3(s3_order_address)
    cleaned_products_df = cleaner.clean_products_data(products_df)
    check_value(cleaned_products_df, expected_value=1846, label='product')
    output_connector.upload_to_db(cleaned_products_df, output_product_table_name)

def extract_and_clean_order_data():
    data = extractor.read_rds_table(input_connector, input_order_table_name)
    cleaned_data = cleaner.clean_order_data(data)
    check_value(cleaned_data, expected_value=120123, label='order')
    output_connector.upload_to_db(cleaned_data, output_order_table_name)

def extract_and_clean_date_event_data():
    json_data = extractor.extract_from_s3(s3_date_event_address)
    cleaned_data = cleaner.clean_date_event_data(json_data)
    check_value(cleaned_data, expected_value=120123, label='date_event')
    output_connector.upload_to_db(cleaned_data, output_date_event_table_name)


if __name__ == '__main__':
    # Variables
    input_yaml_file = "db_creds.yaml"
    input_user_table_name = "legacy_users"
    input_order_table_name = "orders_table"
    output_yaml_file = "sales_data_db_creds.yaml"
    output_user_table_name = "dim_users"
    output_card_table_name = "dim_card_details"
    output_store_table_name = "dim_store_details"
    output_product_table_name = "dim_products"
    output_order_table_name = "orders_table"
    output_date_event_table_name = "dim_date_times"
    PDF_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    s3_order_address = "s3://data-handling-public/products.csv"
    s3_date_event_address = 's3://data-handling-public/date_details.json'

    headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"


    # Class instances
    input_connector = DatabaseConnector(input_yaml_file)
    output_connector = DatabaseConnector(output_yaml_file)
    extractor = DataExtractor()
    cleaner = DataCleaning()

    # input_connector.list_db_tables()
    # extract_and_clean_user_data()
    # extract_and_clean_card_data()
    extract_and_clean_store_details()
    # extract_and_clean_product_details()
    # extract_and_clean_order_data()
    # extract_and_clean_date_event_data()



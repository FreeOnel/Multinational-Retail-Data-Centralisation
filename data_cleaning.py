import pandas as pd

class DataCleaning:

    def clean_user_data(self, df: pd.DataFrame):
        '''
        - **Change "NULL" strings data type into NULL data type**
        - **Remove NULL values**
        - **Convert "join_data" column into a datetime data type**
        '''
        df.replace("NULL", pd.NA, inplace=True)
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce',format='mixed')
        df.dropna(inplace=True)
        
        return df
    
    def clean_card_data(self, df: pd.DataFrame):
        '''
        - **Change "NULL" strings data type into NULL data type**
        - **Remove NULL values**
        - **Remove duplicate card numbers**
        - **Remove non-numerical card numbers**
        - **Convert "date_payment_confirmed" column into a datetime data type**
        '''
        df.replace("NULL", pd.NA, inplace=True)
        df['card_number'] = df['card_number'].astype(str).str.extract('(\d+)')[0]
        # df['card_number'] = pd.to_numeric(df['card_number'], errors='ignore') # ????????
        df.drop_duplicates(subset='card_number', inplace=True)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed', errors='coerce')
        df.dropna(inplace=True)

        return df

    def clean_store_data(self, df: pd.DataFrame):
        '''
        - **Change "NULL" strings data type into NULL data type**
        - **Remove NULL values**
        - **Convert "opening_date" column into a datetime data type**
        - **Strip away symbols, letters, and white spaces from "staff_numbers" column**
        '''
        df.drop(columns='lat', inplace=True) # contains mostly 'None'
        df.loc[0, 'latitude'] = "N/A" # sets latitude of 0th row to NA otherwise it will be dropped (needed in future task)
        df.replace("NULL", pd.NA, inplace=True)
        df['opening_date'] = pd.to_datetime(df['opening_date'], format='mixed', errors='coerce')
        df['staff_numbers'] = df['staff_numbers'].astype(str).str.replace(r'\D', '', regex=True)
        df.dropna(inplace=True)
        print(df)

        return df

    def convert_product_weights(self, df):
        '''
        - **Change "NULL" strings data type into NULL data type**
        - **Remove NULL values**
        - **Convert all weight into kg units**
'''
        def convert_to_kg(weight):
            weight = weight.replace(' ', '')  # Remove all spaces
            if 'x' in weight:  # Handling cases like '12 x 100 g'
                try:
                    parts = weight.split('x')
                    if len(parts) == 2:
                        weight_value = float(parts[0].strip()) * float(parts[1].strip().replace('g', '').replace('kg', '').replace('ml', '').strip())
                        return weight_value / 1000  # Convert to kg if it's in grams or milliliters
                    else:
                        return None
                except ValueError:
                    print("x error")
                    return None  # In case of any malformed string          
                 
            if 'kg' in weight:
                return float(weight.replace('kg', '').strip())
            elif 'g' in weight:
                return float(weight.replace('g', '').strip()) / 1000
            elif 'ml' in weight:
                return float(weight.replace('ml', '').strip()) / 1000
            elif 'oz' in weight:
                return float(weight.replace('oz', '').strip()) * 0.0283495
            else:
                return None
        
        df['weight'] = df['weight'].astype(str).apply(convert_to_kg)
        df.dropna(subset=['weight'], inplace=True)
        return df
    
    def clean_products_data(self, df):
        df.replace("NULL", pd.NA, inplace=True)
        df = self.convert_product_weights(df)
        df.dropna(inplace=True)
        return df

    def clean_order_data(self, df: pd.DataFrame):
        '''
        - **Remove unwanted columns**
        '''
        df.drop(columns=['first_name','last_name', '1'], inplace=True)
        
        return df

    def clean_date_event_data(self, df):
        '''
        - **Change "NULL" strings data type into NULL data type**
        - **Remove NULL values**
        - **Convert values in columns "day", "month", and "year" into numeric values. Values that can't be converted should be converted into NaN**
        '''
        df.replace("NULL", pd.NA, inplace=True)
        for column in ['day', 'month', 'year']:
            df[column] = pd.to_numeric(df[column], errors='coerce')
        
        df.dropna(inplace=True)

        return df



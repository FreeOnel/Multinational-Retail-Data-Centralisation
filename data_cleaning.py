import pandas as pd

class DataCleaning:

    def clean_user_data(self, df: pd.DataFrame):
        df.replace("NULL", pd.NA, inplace=True)
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed', errors='coerce')
        df.dropna(inplace=True)
        
        return df
    
    def clean_card_data(self, df: pd.DataFrame):
        df.replace("NULL", pd.NA, inplace=True)
        df.dropna(inplace=True)

        df = df[df['card_number'].apply(lambda x: str(x).isnumeric())]
        df.drop_duplicates(subset='card_number', inplace=True)

        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], format='mixed',  errors='coerce')

        return df


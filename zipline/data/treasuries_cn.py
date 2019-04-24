import pandas as pd

def earliest_possible_date():
    return pd.Timestamp('1990', tz='UTC')

def get_treasury_data(start_date, end_date):
    idx = pd.date_range(start_date, end_date)
    cols = [
        '1month', '3month', '6month', 
        '1year', '2year', '3year', '5year', '7year', '10year', '20year', '30year',
    ]
    return pd.DataFrame(index=idx, columns=cols)

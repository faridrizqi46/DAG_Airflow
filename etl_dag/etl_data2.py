import requests
import token_api
import pandas as pd
import mysql.connector as mcon
from datetime import datetime
import datetime

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False 

    if df.isnull().values.any():
        raise Exception("Null values found")

    return True

def run_etl():
    # Extract
    url = 'https://api.spoonacular.com/recipes/complexSearch'
    params = {'apiKey': token_api.TOKEN, 'cusine':'indonesia'}
    r = requests.get(url, params=params)
    df = pd.json_normalize(data=r.json()['results'])
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    df['date'] = today
    
    # Transform/Validate
    if check_if_valid_data(df):
        print("Data valid, proceed to Load stage")
        
    # Load
    try:
        db = mcon.connect(host=token_api.HOST_NAME_mysql, user=token_api.USERNAME_mysql, password=token_api.PASSWORD_mysql, database=token_api.DBNAME_mysql)
        print('Success connect to db')
    except:
        raise Exception('Failed to connect to database')
    cursor = db.cursor()
    sql = """CREATE TABLE IF NOT EXISTS food (
                    date DATE NOT NULL,
                    food_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    image TEXT NOT NULL,
                    image_type TEXT NOT NULL
            )"""
    cursor.execute(sql)
    for i,row in df.iterrows():
        insert_into_videos = ("""INSERT INTO food (date, food_id, title, image, image_type)
                                VALUES(%s,%s,%s,%s,%s);""")
        row_to_insert = (row[-1], row[0], row[1], row[2], row[3])
        cursor.execute(insert_into_videos, row_to_insert)
        print(f'Insert data-{i}')
        
    db.commit()
    db.close()
    print('close database successfully')

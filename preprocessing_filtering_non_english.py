import pandas as pd
from langdetect import detect
from multiprocessing import Pool
import mysql.connector
from concurrent.futures import ThreadPoolExecutor
import concurrent
from sqlalchemy import create_engine
from tqdm import tqdm


def insert_into_mysql(df, table_name, connection, chunksize=1000):
    # cursor = connection.cursor()
    for i in range(0, len(df), chunksize):
        chunk = df[i:i+chunksize]
        chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
        print(f"Inserted {(i+chunksize)} rows")


def contains_english(text):
    try:
        lang = detect(text['comments'])
        # print(f'text: {text[0:50]}   lang:{lang}')
        return lang == 'en'  # 'en' is the language code for English
    except:
        return False  # Consider as non-English if language detection fails

# def process_record(record):
#     text = record['comments']
#     print(text)
#     return contains_non_english(text)

if __name__ == '__main__':
    # Database connection
    connection = mysql.connector.connect(
        host='localhost',
        database='dmdb',
        user='root',
        password='password123'
    )
    engine = create_engine("mysql+mysqlconnector://root:password123@localhost/dmdb")

    # Example SQL query to select data from a table
    query = "SELECT * FROM detailed_reviews"  # Replace 'your_table_name' with your actual table name

    # Read records from the SQL table using pandas
    # df = pd.read_sql(query, connection)
    df = pd.read_sql(query, engine)
    print("df has been created")
    tqdm.pandas(desc="Processing...")
    # df['is_english']=df['comments'].apply(contains_english)
    df['is_english']=df.progress_apply(contains_english,axis=1)
    print(df.head())
    count_true=(df['is_english']==True).sum()
    print(f'count_true: {count_true}')
    newdf=df[df['is_english']]['id']
    # print(newdf.head())
    # newdf.to_sql(name="records_we_want", con=engine, if_exists='replace', index=False)
    insert_into_mysql(newdf, "record_id_we_want", engine, chunksize=10000)

    
    # print(df.head())
    # Close the database connection
    connection.close()

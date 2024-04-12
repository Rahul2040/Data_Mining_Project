import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Function to insert DataFrame into MySQL table in chunks
def insert_into_mysql(df, table_name, connection, chunksize=1000):
    # cursor = connection.cursor()
    for i in range(0, len(df), chunksize):
        chunk = df[i:i+chunksize]
        chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
        print(f"Inserted {(i+chunksize)} rows")
    # cursor.close()

# Establish a connection to MySQL
try:
    connection = mysql.connector.connect(
        host='localhost',
        database='dmdb',
        user='root',
        password='password123'
    )

    if connection.is_connected():
        db_info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_info)
        engine = create_engine("mysql+mysqlconnector://root:password123@localhost/dmdb")
        # Read CSV file into a DataFrame
        df = pd.read_csv('D:/Data Mining/project/dataset/archive/detailed_reviews.csv')

        # Define the name of your table
        table_name = 'detailed_reviews'

        # Call the function to insert DataFrame into MySQL in chunks
        insert_into_mysql(df, table_name, engine, chunksize=10000)

        print("Data inserted into MySQL table successfully!")

except mysql.connector.Error as e:
    print("Error connecting to MySQL", e)

finally:
    # Close database connection
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")

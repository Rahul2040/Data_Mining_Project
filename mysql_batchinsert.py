import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

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

        # Sample DataFrame
        # data = {'Name': ['John', 'Anna', 'Peter', 'Linda'],
        #         'Age': [28, 35, 25, 32],
        #         'City': ['New York', 'Paris', 'London', 'Berlin']}

        df = pd.read_csv("D:/Data Mining/project/dataset/archive/detailed_reviews.csv")
        df1=df[0:10]
        # print(df.head())
        engine = create_engine("mysql+mysqlconnector://root:password123@localhost/dmdb")
        # Insert DataFrame into MySQL table
        df1.to_sql(name='detailed_reviews_1', con=engine, if_exists='replace', index=False)

        print("Data inserted into MySQL table successfully!")

except mysql.connector.Error as e:
    print("Error connecting to MySQL", e)

finally:
    # Close database connection
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")

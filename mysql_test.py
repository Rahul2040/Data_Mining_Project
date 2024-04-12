import mysql.connector

# Establishing a connection to the database
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
        cursor = connection.cursor()

        # Executing a SQL query
        cursor.execute("show tables;")

        # Fetching data from the result
        rows = cursor.fetchall()
        for row in rows:
            print(row)

except mysql.connector.Error as e:
    print("Error connecting to MySQL", e)

finally:
    # Closing database connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

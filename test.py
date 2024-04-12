import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+mysqlconnector://root:password123@localhost/dmdb")

query = "SELECT * FROM comments_in_english"
df = pd.read_sql(query, engine)
print("df has been created")
df.drop(columns=['comments'],inplace=True)
print(df.head())

df.to_csv("only_english_comments.csv")
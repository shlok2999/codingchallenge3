import requests
from bs4 import BeautifulSoup
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

url = "https://books.toscrape.com/"


word_to_num = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5
}

def get_book_details(books,book_details):
  for book in books:
    # Getting Title
    heading = book.find('h3')
    anchors = heading.find('a')
    title = anchors['title']
    book_details['title'].append(title)
    # Getting rating
    rating = book.p['class'][1]
    rating = word_to_num[rating.lower()]
    book_details['rating'].append(rating)
    # Getting Price
    price = book.find('p',class_='price_color')
    price = float(price.text[2:])
    book_details['price'].append(price)
    # Getting Availability
    availability = book.find('p',class_='instock availability')
    availability = availability.text.strip()
    stock = (availability == "In stock")
    book_details['availability'].append(stock)
  return book_details



def get_csv():
  book_details = {
    'title':[],
    'rating':[],
    'price':[],
    'availability':[]
  }
  for page in range(1,51):
    target_page = f"/catalogue/page-{page}.html"
    target_url = url + target_page
    response = requests.get(target_url)
    soup = BeautifulSoup(response.text,"html.parser")
    books = soup.find_all('article',class_='product_pod')
    book_details = get_book_details(books,book_details)

  df = pd.DataFrame.from_dict(book_details)
  df.to_csv('data.csv',index=False)

get_csv()
load_dotenv()
username = os.getenv('SNOWFLAKE_USERNAME')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account
    )

# Create a cursor object
cur = conn.cursor()

# Create a warehouse
cur.execute("CREATE WAREHOUSE IF NOT EXISTS Book_WAREHOUSE")
cur.execute("USE WAREHOUSE Book_WAREHOUSE")

# Create a database
cur.execute("CREATE DATABASE IF NOT EXISTS book_db")
cur.execute("USE DATABASE book_db")

# Create a schema
cur.execute("CREATE SCHEMA IF NOT EXISTS book_schema")
cur.execute("USE SCHEMA book_schema")

# Create a table
table_query = """
CREATE OR REPLACE TABLE Books (
    Title VARCHAR,
    Rating INTEGER,
    Price FLOAT,
    Availability BOOLEAN
)
"""
cur.execute(table_query)

# creating a stage
cur.execute("CREATE OR REPLACE STAGE books_staging")

# loading the data from the csv file to the stage
cur.execute("PUT file://data.csv @books_staging auto_compress=true")

# copying the data from the stage to the table
cur.execute("""COPY INTO Books FROM @books_staging  
            FILE_FORMAT=(TYPE=CSV ,
            FIELD_DELIMITER=',' ,
            SKIP_HEADER=1,
            FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR = CONTINUE""")

# Close the cursor and the connection
cur.close()
conn.close()
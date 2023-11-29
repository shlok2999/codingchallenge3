import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

load_dotenv()
username = os.getenv('SNOWFLAKE_USERNAME')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
warehouse = 'Book_WAREHOUSE'
database = 'book_db'
schema = 'book_schema'
# Establish a connection to Snowflake
con = snowflake.connector.connect(
    user=username,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
)

table_name = 'Books'

# Write a SQL query to fetch data
query = f"SELECT * FROM {table_name}"

# Execute the query and fetch the result into a pandas DataFrame
df = pd.read_sql_query(query, con)

# Close the connection
con.close()

# df = pd.read_csv('data2.csv')

print(df.head())
# Set the title of the app
st.title("Book Dashboard Using Streamlit")

# Calculate KPIs
num_books = len(df)
avg_price = df['PRICE'].mean()
avg_rating = df['RATING'].mean()

# Create three columns for KPIs
col1, col2, col3 = st.columns(3)

# Define a function to create a styled box
def create_box(text, value):
    return f'<div style="border:2px solid black; padding:10px; color: black; background-color: #6DB9EF;"><h4>{text}</h4><h2>{value}</h2></div>'

# Display KPIs in separate boxes
with col1:
    st.markdown(create_box('Number of Books:', num_books), unsafe_allow_html=True)

with col2:
    st.markdown(create_box('Average Price:', f'{avg_price:.2f}'), unsafe_allow_html=True)

with col3:
    st.markdown(create_box('Average Rating:', f'{avg_rating:.2f}'), unsafe_allow_html=True)

# Count the number of available and unavailable books
availability_counts = df['AVAILABILITY'].value_counts()

# Ensure that both 'True' and 'False' indices exist
availability_counts = availability_counts.reindex([True, False], fill_value=0)

# Create a pie chart
fig, ax = plt.subplots()
ax.pie(availability_counts, labels=['Available', 'Unavailable'], autopct='%1.1f%%')

# Add a legend
ax.legend()

# # Add border to the figure
# fig.patch.set_edgecolor('black')  
# fig.patch.set_linewidth('2')

# Display the pie chart with Streamlit
st.pyplot(fig)

# Get top 5 books based on price
top5_price = df.nlargest(5, 'PRICE')

# Create a bar chart for top 5 books based on price
fig1 = px.bar(top5_price, x='TITLE', y='PRICE', title='Top 5 Books Based on Price')
st.plotly_chart(fig1)

# Get the count of books for each rating
rating_counts = df['RATING'].value_counts().sort_index()

# Create a bar chart for the count of books for each rating
fig2 = px.bar(rating_counts, x=rating_counts.index, y=rating_counts.values, labels={'x':'Rating', 'y':'Count'}, title='Count of Books for Each Rating')

st.plotly_chart(fig2)

# Create a dropdown menu with book titles
selected_book = st.selectbox('Select a book:', df['TITLE'].unique())

# Get the details of the selected book
book_details = df[df['TITLE'] == selected_book][['TITLE', 'PRICE', 'RATING', 'AVAILABILITY']].iloc[0]

# Display information for the selected book as a DataFrame
st.dataframe(pd.DataFrame(book_details).T)

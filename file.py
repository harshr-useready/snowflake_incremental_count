import streamlit as st
import pandas as pd
from datetime import date
import snowflake.connector

conn = snowflake.connector.connect(
    user="rajattrial",
    password="Amazon@123",
    account="iewdatu-ur46975",
    database="snowflake",
    schema="account_usage",
)

st.write("#### Query Result to CSV Updater")
date = st.date_input("Select Your Date", value=date.today())

st.cache_data(ttl=9000)
def query_executor():
    query = f"""SELECT count(*) as "{date}" FROM query_history where START_TIME > '{date}'"""
    results = pd.read_sql(query, conn)
    return results

# Read the existing CSV file (if it exists)
try:
    df = pd.read_csv('my_csv_file.csv')
except FileNotFoundError:
    df = pd.DataFrame()

if st.button("Run Query"):
    query_result = query_executor()
    st.success('Query Run Successfully!', icon="✅")
    st.write("#### Query Result")
    st.dataframe(query_result, hide_index=True)

if st.button("Insert Query result into CSV"):
    query_result = query_executor()
    df[query_result.columns[0]] = query_result.iloc[0, 0]
    df.to_csv('my_csv_file.csv', index=False)
    st.success('Data Inserted Successfully!', icon="✅")

st.write("#### Latest CSV")
st.dataframe(df, hide_index=True)

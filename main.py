# -*- coding: utf-8 -*-
"""
Created on Sat Oct  7 09:27:13 2023

@author: treha
"""
import streamlit as st
from data import Connect

connection = Connect()

page_title = "EOL Vision Inspection"
layout = "centered"
st.set_page_config(page_title = page_title, layout = layout)
st.title(page_title )

@st.experimental_memo 
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

with st.form("entry_form", clear_on_submit=True):
    col = st.columns(1)
    with st.expander("Date"):
        start_date = st.date_input ( "Start Date" , value=None , min_value=None , max_value=None , key="start_date" )
        end_date = st.date_input ( "Start Date" , value=None , min_value=None , max_value=None , key="end_date" )
        #date = st.text_input("date", placeholder="date", key = "input_date")
        
    submitted = st.form_submit_button("Save Date")
    if submitted:
        s_date = str(st.session_state["start_date"])
        e_date = str(st.session_state["end_date"])
        
        conn = connection.redshift_connection("testlake")
        #df = connection.convert_to_numbers(connection.query_redshift(connection.query(curr_date), conn))
        df = connection.convert_to_numbers(connection.query_redshift(connection.query(s_date, e_date), conn))
        st.write(df)
        csv = convert_df(df)

        

if submitted:
        st.download_button(
           "Press to Download",
           csv,
           "file.csv",
           "text/csv",
           key='download-csv'
        )




        




 


 


                
        






        
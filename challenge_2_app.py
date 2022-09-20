# App file

# ------------------------
# ------ Imports -------
# ------------------------

import os
import datetime
import snowflake.connector

import math as ma
import pandas as pd
import streamlit as st
import plotly.express as px


# ------------------------
# ------ Functions -------
# ------------------------


# Fetch Snowflake data

def execute_sf_query_table(query):
    # Connect to Snowflake
    my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])

    with my_cnx.cursor() as my_cur:

        # Execute the query
        my_cur.execute(query)

        # Get the table header
        header = [x[0] for x in my_cur.description]

        # get the table rows
        rows = my_cur.fetchall()
        
    # Close the query
    my_cnx.close()

    # Return the query in a dataframe
    return(pd.DataFrame(rows, columns = header))


# Get a table in snowflake based on its name

def get_table(table_name, limit):
    if type(limit) == int:
        return(execute_sf_query_table("select * from " + table_name + " limit "+ str(limit)))
    else:
        return(execute_sf_query_table("select * from " + table_name + ""))


# ---------------------------------------------------------------------------------------------------------
#  Main display 
# ---------------------------------------------------------------------------------------------------------

st.title("🎅 D&A Challenge - 2 🎅")

# Display Dataset
st.header('Data received')
st.text('Here is a snapshot of the data provided for this exercise.')

# Snowflake Query 
if st.button("Display the sales dataset"):
    st.table(get_table("sales", 5))


# ---------------------------------------------------------------------------------------------------------
# Exercise 1 - query the data to count the number of appartments sold between two dates
# ---------------------------------------------------------------------------------------------------------

# Title
st.markdown("""---""")
st.header('Q1 - Housing sales between two dates 📆')

# User Input
col_1, col_2 = st.columns(2)

# Select the first date
d1 = col_1.date_input(
     "First day",
     datetime.date(2020, 1, 1)
     )

# Select the second date
d2 = col_2.date_input(
     "Last day",
     datetime.date(2020, 3, 31)
     )

# Snowflake Query 
my_query_results = execute_sf_query_table("select transaction_date, local_type, sum(count(*)) over (partition by transaction_date, local_type) as daily_sales_count from sales where (transaction_date <= '"+d2.strftime('%Y-%m-%d')+"' and transaction_date >= '"+d1.strftime('%Y-%m-%d')+"') group by transaction_date, local_type order by transaction_date asc")

# Exercise Answer
st.text('')
st.write('**' + str(sum((my_query_results[my_query_results['LOCAL_TYPE']=='Appartement']['DAILY_SALES_COUNT'].to_list()))) + '**' + ' appartments have been sold during this period of time')

# Plot Chart
fig = px.bar(
    my_query_results, 
    x = "TRANSACTION_DATE", 
    y = "DAILY_SALES_COUNT", 
    color = "LOCAL_TYPE", 
    title = "Daily housing sales from " + d1.strftime('%Y-%m-%d') + " to " + d2.strftime('%Y-%m-%d') + " by housing type"
    )
fig.show()
st.plotly_chart(fig)


# ---------------------------------------------------------------------------------------------------------
# Exercise 2 - get the ratio of sales per room number
# ---------------------------------------------------------------------------------------------------------

# Title
st.markdown("""---""")
st.header('Q2 - Flat sales per room number #️⃣')

# Snowflake Query 
my_query_results_2 = execute_sf_query_table("""
    select 
        room_number,
        round(avg(carrez_surface)) as avg_surface,
        count(*) as total_sales,
        
        from sales
    
    where local_type = 'Appartement'
    group by room_number
    order by room_number;
    """)

st.table(my_query_results_2)

# Exercise Answer
fig2 = px.pie(
    my_query_results_2, 
    values = 'TOTAL_SALES', 
    names = 'ROOM_NUMBER', 
    title = 'Flat sales by room number'
    )
fig2.show()
st.plotly_chart(fig2)


# ---------------------------------------------------------------------------------------------------------
# Exercise 3 - get the top x higher-priced regions
# ---------------------------------------------------------------------------------------------------------

# Title
st.header('Thrid query: Average squarred meter price per department 💵')

# Snowflake Query 
my_query_results_3 = execute_sf_query_table("select dept_code, avg(transaction_value/carrez_surface) as avg_sqm_price from sales group by dept_code order by avg_sqm_price desc")

# Data formating
my_query_results_3['AVG_SQM_PRICE'] = my_query_results_3['AVG_SQM_PRICE'].apply(ma.ceil)

st.text('This will display a top of the higher priced departments. Please select the number of departments you want to see.')
default = my_query_results_3 if len(my_query_results_3) <= 10 else 10
top = st.slider('How many departments do you want to see?', 0, len(my_query_results_3), default)

# Exercise Answer
#st.dataframe(my_query_results_3[:top].set_index('DEPT_CODE'))
fig3 = px.bar(my_query_results_3[:top], x="DEPT_CODE", y="AVG_SQM_PRICE", title="Average squarred meter price per department")
fig3.show()
st.plotly_chart(fig3)


# ---------------------------------------------------------------------------------------------------------
# Exercise 4 - get the average square meter price per region
# ---------------------------------------------------------------------------------------------------------

# Exercise title
st.header('Fourth query: Average squarred meter price per region 🏡/🏢')

# Dept code input
region_list = execute_sf_query_table("select distinct new_region from dept_info")['NEW_REGION'].to_list()
selected_region = st.selectbox("Please select the region you want to study", region_list)

# Snowflake query
dept_list = execute_sf_query_table("select insee_code from dept_info where new_region ='"+str(selected_region).replace("'","''")+"'")['INSEE_CODE'].to_list()
my_query_results_4 = execute_sf_query_table("select local_type, avg(transaction_value/carrez_surface) as avg_sqm_price from sales where dept_code in ("+str(dept_list).replace('[','').replace(']','')+") group by local_type order by avg_sqm_price desc")

# Display the different average prices with metrics
col1, col2 = st.columns(2)
col1.metric("🏡", str(int(my_query_results_4[my_query_results_4['LOCAL_TYPE']=='Maison']['AVG_SQM_PRICE'].values[0]))+ " €")
col2.metric("🏢", str(int(my_query_results_4[my_query_results_4['LOCAL_TYPE']=='Appartement']['AVG_SQM_PRICE'].values[0]))+ " €")

# ------------------------
# Fifth exercise, get the top 10 higher-priced appartments
# ------------------------

# Exercise title
st.header('Fifth query: Top 10 higher priced appartments sold 🏢')

# Answer the question
st.dataframe(execute_sf_query_table("select transaction_value, street_number, street_type, city_name, dept_code, carrez_surface, room_number from sales where (transaction_value is not null and local_type='Appartement') order by transaction_value desc limit 10"))

# ------------------------
# Sixth exercise, get the sales number evolution
# ------------------------

# Exercise title
st.header('Sixth query: Sales number evolution for the second quarter 📈')

# Answer the question
first_sem_sales_count = execute_sf_query_table("select count(*) from sales where (transaction_date>='2020-01-01' and transaction_date<'2020-03-31')").values[0][0]
second_sem_sales_count = execute_sf_query_table("select count(*) from sales where (transaction_date>='2020-04-01' and transaction_date<='2020-07-31')").values[0][0]
st.metric("Second semester sales number",second_sem_sales_count, str(int(second_sem_sales_count - first_sem_sales_count))+ ' ('+str(round((second_sem_sales_count - first_sem_sales_count)*100/first_sem_sales_count, 2))+" %)")

# ------------------------
# Seventh exercise, get thesales number evolution
# ------------------------

# Exercise title
st.header('Seventh query: Departments with a high sales number increase between the first and the second semester 💸')

# Answer the question
df_7 = execute_sf_query_table("select dept_code, date_part(quarter,transaction_date::date) as t_quarter, sum(count(*)) over (partition by dept_code, t_quarter) as sales_count from sales group by dept_code, t_quarter")

# Split the df per semester
df_7_1 = df_7[df_7['T_QUARTER']==1].dropna()
df_7_2 = df_7[df_7['T_QUARTER']==2]

# Merge the dict again
df_7 = df_7_1.merge(df_7_2, on='DEPT_CODE', how='left').fillna(0)

# Rename the columns and drop the quarters
df_7 = df_7.rename({'SALES_COUNT_x':'SALES_COUNT_Q1', 'SALES_COUNT_y': 'SALES_COUNT_Q2'}, axis=1)
df_7 = df_7.drop(['T_QUARTER_x', 'T_QUARTER_y'], axis=1)

# Change the nulmber format
df_7['SALES_COUNT_Q2'] = df_7['SALES_COUNT_Q2'].astype(int)

# Add the evolution column
df_7['EVOL (%)'] = 100*round((df_7['SALES_COUNT_Q2']-df_7['SALES_COUNT_Q1'])/ df_7['SALES_COUNT_Q1'],4)
df_7['EVOL (%)'] = df_7['EVOL (%)'].astype(int)

st.dataframe(df_7[df_7['EVOL (%)']>20].sort_values('EVOL (%)', ascending=False))

# ------------------------
# Eighth exercise, get the average price difference between appartments with 2 rooms and the ones with 3 rooms
# ------------------------

# Exercise title
st.header('Eighth query: Average price difference between appartments with 🥈 rooms and the ones with 🥉 rooms')

# Answer the question
two_rooms_avg_sqm_price = execute_sf_query_table("select avg(transaction_value/carrez_surface) as avg_sqm_price from sales where room_number=2").values[0][0]
three_rooms_avg_sqm_price = execute_sf_query_table("select avg(transaction_value/carrez_surface) as avg_sqm_price from sales where room_number=3").values[0][0]

# Display the different average prices with metrics
col1, col2 = st.columns(2)
col1.metric("2-rooms 🥈 avg sqm price", str(int(two_rooms_avg_sqm_price))+ " €")
col2.metric("3-rooms 🥉 avg sqm price", str(int(three_rooms_avg_sqm_price))+ " €", str(100*round((three_rooms_avg_sqm_price-two_rooms_avg_sqm_price)/two_rooms_avg_sqm_price,2))+ " %")


# ------------------------
# Ninth exercise, get the average price of the higher-priced cities in a multi-selection of departments
# ------------------------

# Exercise title
st.header('Ninth query: Average price of the 10 higher-priced cities in a multi-selection of departments ✨')

# Query the list of departments
dept_list = execute_sf_query_table("select distinct dept_code from sales")['DEPT_CODE'].to_list()
selected_dept_list = st.multiselect("Please select the departments you want to study", dept_list, default=['06', '13', '33', '59', '69'])

# Answer the question
st.dataframe(execute_sf_query_table("select city_name, dept_code, round(avg(transaction_value)) as avg_price from sales where dept_code in  ("+str(selected_dept_list).replace('[','').replace(']','')+') group by city_name, dept_code order by avg_price desc limit 10'))

# Don't run anything past here while troubleshooting
st.stop()

#https://poux-be-ds-chal-2-app-streamlit-app-w30f4m.streamlitapp.com/

# old code to have a map
# import folium
# from streamlit_folium import st_folium
# Load the department informations
# df_departement=get_table('dept_info', None)

# Left join to add the department informations
# my_query_results = my_query_results.merge(df_departement, left_on=['DEPT_CODE'], right_on=['INSEE_CODE'], how='left')

# # Print merged table
# st.dataframe(my_query_results)

# # Map initialisation
# map = folium.Map(location=[43.634, 1.433333],zoom_start=6)

# # Transform dataframe into lists
# lat_list = my_query_results['LAT'].to_list()
# lon_list = my_query_results['LON'].to_list()
# name_list = my_query_results['NAME'].to_list()
# lat_lon_list= []
# sqm_price_list = my_query_results['AVG_SQM_PRICE'].tolist()

# # For all the departments
# for i in range(len(lat_list)):
#     lat_lon_list.append([lat_list[i],lon_list[i]])

# # Add markers
# for i in range(len(lat_list)):
#     folium.Marker(lat_lon_list[i],popup='Prix moyen dans le département {} : {}€/m²'.format(name_list[i],sqm_price_list[i])).add_to(map)

# #Print the map on the app
# st_folium(map, width = 725)
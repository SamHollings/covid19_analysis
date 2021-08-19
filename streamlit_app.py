"""This code makes a simple streamlit app for viewing the covid data"""
import pandas as pd

import covid_data
import streamlit as st
import plotly.express as px

data_pack = covid_data.get_data()

st.write('# COVID-19 Dashboard')

df_uk_data = pd.concat([data_pack['uk_nhse_feed'],
                data_pack['england_nhse_feed']],axis=0)#.set_index(['areatype', 'date', 'name'])

fig = px.line(df_uk_data, x="date", y="newPillarTwoTestsByPublishDate",
              color='areatype' ,
              title='Pillar 2 tests by published date')

st.write(fig)

# region cases graph
df_regions = data_pack['region_nhse_feed'].query("date > '2020-04-10'")
fig_region_cases = px.line(df_regions, x='date', y='newCasesByPublishDate', color='name', title='newCasesByPublishDate per Region')

st.write(fig_region_cases)

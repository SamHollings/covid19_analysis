"""This code makes a simple streamlit app for viewing the covid data"""
import pandas as pd

import covid_data
import streamlit as st
import plotly.express as px

data_pack = covid_data.get_data()

df = pd.concat([data_pack['uk_nhse_feed'],
                data_pack['england_nhse_feed']],axis=0)#.set_index(['areatype', 'date', 'name'])

fig = px.line(df, x="date", y="newPillarTwoTestsByPublishDate",
              color='areatype' ,
              title='Pillar 2 tests by published date')

st.write('# COVID-19 Dashboard')
st.write(fig)


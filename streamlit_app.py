"""This code makes a simple streamlit app for viewing the covid data"""
import pandas as pd
import numpy as np
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

# scale data by UK-wide number of tests
df_regions_scaled = (df_regions[['date', 'name','code', 'newCasesByPublishDate']]
                     .merge(data_pack['uk_nhse_feed'][['newPillarTwoTestsByPublishDate','date']], on='date'))
df_regions_scaled['newCases_perUKPillarTwoTest'] = df_regions_scaled['newCasesByPublishDate'].div(df_regions_scaled['newPillarTwoTestsByPublishDate'])
fig_region_cases = px.line(df_regions_scaled, x='date', y='newCases_perUKPillarTwoTest', color='name', title='newCasesByPublishDate per Region per UK Pillar 2 test count')
st.write(fig_region_cases)

st.write("### simple map")

import geopandas as gpd

df_local_auth_shapes = gpd.read_file('./shapefiles/Local_Authority_Districts_(December_2020)_UK_BGC.geojson').set_index('LAD20CD')

fig_regions_scaled_cases_map = px.choropleth(df_local_auth_shapes,
              geojson=df_local_auth_shapes.geometry,
              locations=df_local_auth_shapes.index,
              color=np.random.randn(len(df_local_auth_shapes)),
              projection="mercator")
fig_regions_scaled_cases_map.update_geos(fitbounds="locations", visible=False)

st.write(fig_regions_scaled_cases_map)
print()



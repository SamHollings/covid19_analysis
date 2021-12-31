"""This code makes a simple streamlit app for viewing the covid data"""
import pandas as pd
#import numpy as np
#import covid_data
import streamlit as st
import plotly.express as px
import numpy as np
#import geopandas as gpd

#data_pack = covid_data.get_data()
st.set_page_config(layout='wide')
st.write('# COVID-19 Dashboard')

def get_nhse_feed_local_data():
    """Gets the local csv files for the NHSE feed and combines them into one pandas dataframe"""
    df = pd.concat([
                pd.read_csv("data/uk_nhse_feed.csv"),#.tail(100),
                pd.read_csv("data/england_nhse_feed.csv"),#.tail(100),
                pd.read_csv("data/region_nhse_feed.csv"),
                pd.read_csv("data/nhsregion_nhse_feed.csv"),
                pd.read_csv("data/utla_nhse_feed.csv"),#.tail(100),
                pd.read_csv("data/ltla_nhse_feed.csv"),#.tail(100),
                    ],
               axis=0)
    return df

def filter_english_only(df):
    return df[~df['code'].str.contains('[WSN]')]


df = get_nhse_feed_local_data()
df = filter_english_only(df)


col1, col2, col3 = st.columns(3)

# df_uk_data = df[df['areatype'].isin(['nation','overview'])]#.set_index(['areatype', 'date', 'name'])
#
# #df_uk_data = pd.concat([data_pack['uk_nhse_feed'],data_pack['england_nhse_feed']],axis=0)#.set_index(['areatype', 'date', 'name'])
#
# fig = px.line(df_uk_data, x="date", y="newPillarTwoTestsByPublishDate",
#               color='areatype',
#               title='Pillar 2 tests by published date')
#
# st.write(fig)

areatype_options = df['areatype'].unique()
areatype_selection = col1.selectbox('areatype', areatype_options, index=0, key=None, help=None, on_change=None, args=None, kwargs=None)

areaname_options = df[df['areatype'].isin([areatype_selection])]['name'].unique().tolist()
if len(areaname_options) < 10:
    default_areaname_options = areaname_options
else:
    default_areaname_options = areaname_options[:10]

areaname_multiselection = col2.multiselect('areaname', areaname_options,default_areaname_options)
if len(areaname_multiselection) < 1:
    areaname_multiselection = default_areaname_options

metric_options = (df[df['areatype'].isin([areatype_selection]) & df['name'].isin(areaname_multiselection)]
                  .dropna(axis=1, how='all')
                  .drop(['areatype','date','name','code'],axis=1).columns.tolist()
)
metric_selection_default = metric_options,metric_options[0]
metric_selection = col3.multiselect('metric', metric_options,metric_options[0])
if len(metric_selection) < 1:
    metric_selection = metric_selection_default[0]


# region cases graph
df_regions = (df[df['areatype'].isin([areatype_selection])
                 & df['name'].isin(areaname_multiselection)
                 ]
              .set_index(['date','areatype','name'])
              .rename_axis(columns='metric')[metric_selection]
              .stack().rename('value').reset_index()
              )

date_options = np.sort(df_regions['date'].unique())
date_from_index_lambda = lambda x: date_options[x]

date_range = st.select_slider('date_range', options=list(range(0,len(date_options))), value=(0, len(date_options)-1),format_func=date_from_index_lambda)

df_regions_within_date_range = df_regions[df_regions['date'].isin(date_options[date_range[0]:date_range[1]])]

fig_region_cases = px.line(df_regions_within_date_range,
                           x='date',
                           y='value',
                           color='name',
                           line_dash='metric',
                           title=f'{metric_selection} per {areatype_selection}')

fig_region_cases.update_layout(legend=dict(
    font=dict(
            #family="Courier",
            size=10,
        ),
    bgcolor='rgba(0,0,0,0)',
))

st.plotly_chart(fig_region_cases,use_container_width=True)
# #
# # # scale data by UK-wide number of tests
# # df_regions_scaled = (df_regions[['date', 'name','code', 'newCasesByPublishDate']]
# #                      .merge(data_pack['uk_nhse_feed'][['newPillarTwoTestsByPublishDate','date']], on='date'))
# # df_regions_scaled['newCases_perUKPillarTwoTest'] = df_regions_scaled['newCasesByPublishDate'].div(df_regions_scaled['newPillarTwoTestsByPublishDate'])
# # fig_region_cases = px.line(df_regions_scaled, x='date', y='newCases_perUKPillarTwoTest', color='name', title='newCasesByPublishDate per Region per UK Pillar 2 test count')
# # st.write(fig_region_cases)
# #
# # st.write("### simple map")
# #
# # df_local_auth_shapes = gpd.read_file('./shapefiles/Local_Authority_Districts_(December_2020)_UK_BGC.geojson').set_index('LAD20CD')
# #
# # # ToDo: bring in ltla figures to plot on the map.
# #
# # fig_regions_scaled_cases_map = px.choropleth(df_local_auth_shapes,
# #               geojson=df_local_auth_shapes.geometry,
# #               locations=df_local_auth_shapes.index,
# #               color=np.random.randn(len(df_local_auth_shapes)),
# #               projection="mercator")
# # fig_regions_scaled_cases_map.update_geos(fitbounds="locations", visible=False)
# #
# # st.write(fig_regions_scaled_cases_map)
# print()
#
#

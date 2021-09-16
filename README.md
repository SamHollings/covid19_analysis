# covid19_analysis
Tools for NHS-related COVID-19 data analysis. Image below from NHSE&I feed (functions below):
![ARIMA plot of mechanically ventilated beds in england](https://github.com/SamHollings/covid19_analysis/blob/main/graphs/ARIMA_mv_beds_england.png?raw=true)
![four plots of covid data](https://github.com/SamHollings/covid19_analysis/blob/main/graphs/covid_data.png?raw=true)

## ToDo:
- **productionise ETL processes to pull data**
  - make functions (which should be tested)
    - going to need standard input output functions to read\write from which ever technology I choose into pyspark
    - then I can mostly code everything in pyspark
  - make triggers which can activate these
  - designate a warehouse into which this stuff goes.
- **begin construction of a feature store**
  - currently the processed data is all over the place
  - instead need to organise it into spark functions, which act as features against some entity (i.e. geographical region at a point in time)
  - then make "feature process" ETLs to pull from the warehouse and deposit this feature in the feature store
- **productionise front end code / data science code**
  - once the features are arranged better in the feature store, any front end should just scoop them up and display them, run models, etc.
- **look at integrating AWS into all of the above**


## Manifest:
- **covid_data.py** - this contains functions for extracting data from the NHSE API, Google Mobility and Apple Mobility
- **data_tools.py** - functions for manipulating dataframes
- **ARIMA_tools.py** - functions to help with ARIMA modelling
- **plot_tools.py** - functions to help make plots

## Examples:
Pulling the data and making the above plot:
```
import covid_data

covid_data_blob = covid_data.covid_england_data_blob()

df_england_nhse_feed =  covid_data_blob['england_nhse']
df_uk_wide_nhse_feed = covid_data_blob['uk_nhse']
df_nhsregion_nhse_feed = covid_data_blob['nhsregion_nhse']
df_region_nhse_feed = covid_data_blob['region_nhse']
df_utla_nhse_feed = covid_data_blob['utla_nhse']
df_ltla_nhse_feed = covid_data_blob['ltla_nhse']
df_gb_google_mobility_report = covid_data_blob['google_mobility']
df_apple_mobility_report = covid_data_blob['apple_mobility']

# Make some plots of the data
import matplotlib.pyplot as plt
fig, ax = plt.subplots(2,2,figsize = (20,10));
# NHS England tests data
df_england_nhse_feed.set_index('date').newAdmissions.rename("newAdmissions England").plot(ax=ax[0][0]);
df_uk_wide_nhse_feed.set_index('date').newAdmissions.rename("newAdmissions UK").plot(ax=ax[0][0]);
df_england_nhse_feed.set_index('date').newDeaths28DaysByPublishDate.rename("newDeaths28DaysByPublishDate England").plot(ax=ax[0][0]);
df_uk_wide_nhse_feed.set_index('date').newDeaths28DaysByPublishDate.rename("newDeaths28DaysByPublishDate UK").plot(ax=ax[0][0]);
ax[0][0].set_title("new hospital admissions and deaths England and UK")
ax[0][0].legend()
# Google mobility
(df_gb_google_mobility_report[df_gb_google_mobility_report.sub_region_1.isna()]
 .set_index('date')[['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline']]).plot(ax=ax[0][1], title='Google mobility England');

df_nhsregion_nhse_feed.set_index(['date','name'])[['covidOccupiedMVBeds']].unstack().rolling(7).sum().plot(ax=ax[1][0],title='Mech Vent Beds England Regions');
df_apple_mobility_report[(df_apple_mobility_report['region'] == 'England')].set_index('date')[['driving','walking','transit']].plot(ax=ax[1][1], title='Apple Mobility England')
plt.show()
```
Interpolation of the start of any missing data (very simple, just using pandas interpolate "pchip")
![Interpolated plot of covid data](https://github.com/SamHollings/covid19_analysis/blob/main/graphs/covid_data_interpolated.png?raw=true)

```import matplotlib.pyplot as plt
import covid_data

covid_data_blob = covid_data.covid_england_data_blob()

df_england_nhse_feed =  covid_data_blob['england_nhse']

# interpolate and plot
fig,ax = plt.subplots(figsize=(10,5))
for column in ['newAdmissions',	'covidOccupiedMVBeds',	'hospitalCases']:
 interpolate_early_data(df_england_nhse_feed[column]).rename(column).plot(linestyle='--', color=['blue'], linewidth=1);#.loc[60:100]

df_england_nhse_feed[['newAdmissions',	'covidOccupiedMVBeds',	'hospitalCases']].plot(ax=ax,legend=True, title='hospital cases, new admissions and MV beds')```

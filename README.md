# covid19_analysis
Tools for NHS-related COVID-19 data analysis. Image below from NHSE&I feed (functions below):

![alt text](https://github.com/SamHollings/covid19_analysis/blob/main/covid_data.png?raw=true)

## Manifest:
- **covid_data.py** - this contains functions for extracting data from the NHSE API, Google Mobility and Apple Mobility

## Examples:

```
import covid_data

query_structure = {"date": "date",
                   "areatype": "areaType",
                  "name": "areaName",
                  "code": "areaCode",
                  "newAdmissions": "newAdmissions",
                  "newPillarTwoTestsByPublishDate": "newPillarTwoTestsByPublishDate",
                  "plannedCapacityByPublishDate" : "plannedCapacityByPublishDate",
                  "newTestsByPublishDate": "newTestsByPublishDate",
                  "covidOccupiedMVBeds": "covidOccupiedMVBeds",
                   "hospitalCases":"hospitalCases",
                   "newCasesBySpecimenDate":"newCasesBySpecimenDate",
                   "newCasesByPublishDate":"newCasesByPublishDate",
                   "cumCasesByPublishDate":"cumCasesByPublishDate",
                   "newDeaths28DaysByPublishDate":"newDeaths28DaysByPublishDate",
                   "maleCases":"maleCases",
                   "femaleCases":"femaleCases"}

# England Only
df_england_nhse_feed = covid_data.get_paginated_dataset([f"areaType=nation;areaName=england"], query_structure).dropna(how='all',axis=1).sort_values('date')
# All UK (some of the metrics only work for the UK as a whole...)
df_uk_wide_nhse_feed = covid_data.get_paginated_dataset([f"areaType=overview"], query_structure).dropna(how='all',axis=1).sort_values('date')
# NHS Regions
df_nhsregion_nhse_feed = covid_data.get_paginated_dataset([f"areaType=nhsRegion"], query_structure).dropna(how='all',axis=1).sort_values('date')
# Regions (geographic regions) - some different metrics than NHS Regions
df_region_nhse_feed = covid_data.get_paginated_dataset([f"areaType=region"], query_structure).dropna(how='all',axis=1).sort_values('date')

# Changed the structure for these next two filters, because they simply don't have many of the above columns
query_structure_2 = {"date": "date",
                   "areatype": "areaType",
                  "name": "areaName",
                  "code": "areaCode",
                   "newCasesBySpecimenDate":"newCasesBySpecimenDate",
                   "newCasesByPublishDate":"newCasesByPublishDate",
                   "cumCasesByPublishDate":"cumCasesByPublishDate",
                   "newDeaths28DaysByPublishDate":"newDeaths28DaysByPublishDate",}
# upper tier local authorities (counties and unitary authorities, i.e. Lancashire, York, Somerset, etc...)
df_utla_nhse_feed = covid_data.get_paginated_dataset([f"areaType=utla"], query_structure_2).dropna(how='all',axis=1).sort_values('date')
# lower tier local authorities (councils and unitary authorities, i.e. Leeds council, Bradford council, York, etc...)
df_ltla_nhse_feed = covid_data.get_paginated_dataset([f"areaType=ltla"], query_structure_2).dropna(how='all',axis=1)

# google and apple mobility
df_gb_google_mobility_report = covid_data.google_mobility()
df_apple_mobility_report = covid_data.apple_mobility()

# Make some plots of the data
import matplotlib.pyplot as plt
fig, ax = plt.subplots(2,2,figsize = (20,10));
# NHS England tests data
df_england_nhse_feed.set_index('date').newAdmissions.rename("newAdmissions England").plot(ax=ax[0][0]);
df_uk_wide_nhse_feed.set_index('date').newAdmissions.rename("newAdmissions UK").plot(ax=ax[0][0]);
df_england_nhse_feed.set_index('date').newDeaths28DaysByPublishDate.rename("newDeaths28DaysByPublishDate England").plot(ax=ax[0][0]);
df_uk_wide_nhse_feed.set_index('date').newDeaths28DaysByPublishDate.rename("newDeaths28DaysByPublishDate UK").plot(ax=ax[0][0]);
ax[0][0].legend()
# Google mobility
(df_gb_google_mobility_report[df_gb_google_mobility_report.sub_region_1.isna()]
 .set_index('date')[['retail_and_recreation_percent_change_from_baseline',
                    'grocery_and_pharmacy_percent_change_from_baseline',
                    'parks_percent_change_from_baseline',
                    'transit_stations_percent_change_from_baseline',
                    'workplaces_percent_change_from_baseline',
                    'residential_percent_change_from_baseline']]).plot(ax=ax[0][1]);

df_nhsregion_nhse_feed.set_index(['date','name'])[['covidOccupiedMVBeds']].unstack().rolling(7).sum().plot(ax=ax[1][0],title='Mech Vent Beds');
df_nhsregion_nhse_feed.set_index(['date','name'])[['hospitalCases']].unstack().rolling(7).sum().plot(ax=ax[1][1],title='hospital cases');
plt.show()
```

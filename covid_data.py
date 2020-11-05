from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get
from http import HTTPStatus
import pandas as pd
import zipfile
import requests
import io
import re
import numpy.random as random
import numpy as np


def get_paginated_dataset(filters: Iterable[str], structure: Dict[str, Union[dict, str]] = None,
                          start_page = 1, end_page=None) -> pd.DataFrame:
    """This is lifted from the NHSE website: https://coronavirus.data.gov.uk/developers-guide
    The "filters" param is used to determine what geographical level you will pull,
    whilst the "structure" param describes the fields you will pull. The function will loop
    over all the pages requested (or all pages if none specified). 
    ISSUES: The API seems to time out for large datasets (i.e. UTLA), so you might need to pull
    in multiple small batches of 5 or 10 pages at a time.
    -------
    Params
    -------
    filters : list(str,...)
        The geographic area you want. Example: ["areaType=nation;areaName=england"]
        You can choose to not include areaName: ['areaType=nation"].
        Options for areaType: overview, nation, region, nhsRegion, utla, ltla
    structure : dict(str / dict(str))
        The columns you want. You specify it as either just a dictionary full of columm 
        names (the key of the dict defines what the column comes out as for you, so below, 
        the areaName column comes out as "name"):
          {"date": "date",
           "areatype": "areaType",
          "name": "areaName",
          "code": "areaCode",
          "newAdmissions": "newAdmissions"}
        The options you can take are:
        # date - the date of the data point
        # areaType - the area type
        # areaName - area name
        # areaCode - area code (ONS format, i.e. E0000000001). 
        # newCasesByPublishDate - New cases by publish date
        # cumCasesByPublishDate - Cumulative cases by publish date
        # cumCasesBySpecimenDateRate - Rate of cumulative cases by publish date per 100k resident population
        # newCasesBySpecimenDate - New cases by specimen date
        # cumCasesBySpecimenDateRate - Rate of cumulative cases by specimen date per 100k resident population
        # cumCasesBySpecimenDate - Cumulative cases by specimen date
        # maleCases - Male cases (by age)
        # femaleCases - Female cases (by age)
        # newPillarOneTestsByPublishDate - New pillar one tests by publish date
        # cumPillarOneTestsByPublishDate - Cumulative pillar one tests by publish date
        # newPillarTwoTestsByPublishDate - New pillar two tests by publish date
        # cumPillarTwoTestsByPublishDate - Cumulative pillar two tests by publish date
        # newPillarThreeTestsByPublishDate - New pillar three tests by publish date
        # cumPillarThreeTestsByPublishDate - Cumulative pillar three tests by publish date
        # newPillarFourTestsByPublishDate - New pillar four tests by publish date
        # cumPillarFourTestsByPublishDate - Cumulative pillar four tests by publish date
        # newAdmissions - New admissions
        # cumAdmissions - Cumulative number of admissions
        # cumAdmissionsByAge - Cumulative admissions by age
        # cumTestsByPublishDate - Cumulative tests by publish date
        # newTestsByPublishDate - New tests by publish date
        # covidOccupiedMVBeds - COVID-19 occupied beds with mechanical ventilators
        # hospitalCases - Hospital cases
        # plannedCapacityByPublishDate - Planned capacity by publish date
        # newDeaths28DaysByPublishDate - Deaths within 28 days of positive test
        # cumDeaths28DaysByPublishDate - Cumulative deaths within 28 days of positive test
        # cumDeaths28DaysByPublishDateRate - Rate of cumulative deaths within 28 days of positive test per 100k resident population
        # newDeaths28DaysByDeathDate - Deaths within 28 days of positive test by death date
        # cumDeaths28DaysByDeathDate - Cumulative deaths within 28 days of positive test by death date
        # cumDeaths28DaysByDeathDateRate - Rate of cumulative deaths within 28 days of positive test by death date per 100k resident population
    """
    if structure is None:
      structure = {"date": "date",
                   "areatype": "areaType",
                  "name": "areaName",
                  "code": "areaCode",
                  'newCasesByPublishDate' : 'newCasesByPublishDate',
                  'cumCasesByPublishDate' : 'cumCasesByPublishDate',
                  'cumCasesBySpecimenDateRate' : 'cumCasesBySpecimenDateRate',
                  'newCasesBySpecimenDate' : 'newCasesBySpecimenDate',
                  'cumCasesBySpecimenDateRate' : 'cumCasesBySpecimenDateRate',
                  'cumCasesBySpecimenDate' : 'cumCasesBySpecimenDate',
                  'maleCases' : 'maleCases',
                  'femaleCases' : 'femaleCases',
                  'newPillarOneTestsByPublishDate' : 'newPillarOneTestsByPublishDate',
                  'cumPillarOneTestsByPublishDate' : 'cumPillarOneTestsByPublishDate',
                  'newPillarTwoTestsByPublishDate' : 'newPillarTwoTestsByPublishDate',
                  'cumPillarTwoTestsByPublishDate' : 'cumPillarTwoTestsByPublishDate',
                  'newPillarThreeTestsByPublishDate' : 'newPillarThreeTestsByPublishDate',
                  'cumPillarThreeTestsByPublishDate' : 'cumPillarThreeTestsByPublishDate',
                  'newPillarFourTestsByPublishDate' : 'newPillarFourTestsByPublishDate',
                  'cumPillarFourTestsByPublishDate' : 'cumPillarFourTestsByPublishDate',
                  'newAdmissions' : 'newAdmissions',
                  'cumAdmissions' : 'cumAdmissions',
                  'cumAdmissionsByAge' : 'cumAdmissionsByAge',
                  'cumTestsByPublishDate' : 'cumTestsByPublishDate',
                  'newTestsByPublishDate' : 'newTestsByPublishDate',
                  'covidOccupiedMVBeds' : 'covidOccupiedMVBeds',
                  'hospitalCases' : 'hospitalCases',
                  'plannedCapacityByPublishDate' : 'plannedCapacityByPublishDate',
                  'newDeaths28DaysByPublishDate' : 'newDeaths28DaysByPublishDate',
                  'cumDeaths28DaysByPublishDate' : 'cumDeaths28DaysByPublishDate',
                  'cumDeaths28DaysByPublishDateRate' : 'cumDeaths28DaysByPublishDateRate',
                  'newDeaths28DaysByDeathDate' : 'newDeaths28DaysByDeathDate',
                  'cumDeaths28DaysByDeathDate' : 'cumDeaths28DaysByDeathDate',
                  'cumDeaths28DaysByDeathDateRate' : 'cumDeaths28DaysByDeathDateRate',
                  }
    
    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"
    api_params = dict(filters=str.join(";", filters),
                      structure=dumps(structure, separators=(",", ":")), 
                      format="json", page=1)
    data = list()
    page_number = start_page
    current_data = dict(pagination={'next':True}) # dummy initial "next" pagination

    while current_data["pagination"]["next"] is not None:
        api_params["page"] = page_number
        if page_number == end_page: break

        try:
          response = get(endpoint, params=api_params, timeout=10)
        except Exception as error:
          print(f"    Trying page {page_number} again...")
          continue

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        
        data.extend(page_data)

        print(f'{str.join(";", filters)} page {page_number}: {response.url}')
        page_number += 1

    return pd.DataFrame(data)

          
def google_mobility(country_filter="GB"):
  """Pulls data from the google mobility report website https://www.google.com/covid19/mobility/.
  Specify the country to filter by (two character code), i.e. GB for United Kingdom. If you leave 
  it blank, i.e. "", then you get everything. 
  """
  response = requests.get("https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip")
  zip_file = zipfile.ZipFile(io.BytesIO(response.content))
  GB_google_mobility_report_paths = list(filter(lambda x: country_filter in x, zip_file.namelist()))
  df_gb_google_mobility_report = pd.concat([pd.read_csv(zip_file.open(file)) for file in GB_google_mobility_report_paths],axis=0)
  return df_gb_google_mobility_report


def apple_mobility():
  """Pulls data from the apple mobility report website https://covid19.apple.com/mobility
  """
  df_apple_mobility_report = pd.read_csv("https://covid19-static.cdn-apple.com/covid19-mobility-data/2019HotfixDev25/v3/en-us/applemobilitytrends-2020-10-30.csv")
  return df_apple_mobility_report


def interpolate_early_data(series : pd.Series, 
                           zeropoints : list = [-40,-20] ) -> pd.Series:
    """
    Interpolate the from zero up to the start of the data, prefilling the defined 
    "zeropoints" range with 0s (relative to the first filled data point).
    """
    series_original = series.copy()
    series_without_na = series_original.dropna()

    earliest_filled_index = series_without_na.index[0]
    zero_points_index = [earliest_filled_index+zeropoints[0],earliest_filled_index+zeropoints[1]]

    # set some earlier points to zero to mark the area between which we need to interpolate

    empty_series = pd.Series(index=range(zero_points_index[0],earliest_filled_index-1)) # this goes from the zero point up to the start of the data

    series_working = empty_series.append(series.dropna())
    series_working.loc[zero_points_index[0]:zero_points_index[1]] = 0 # the zero point range as 0

    # do the interpolation
    series_working = series_working.interpolate(method='pchip', limit_direction='both', limit=None)

    # add some noise
    noise_scale_factor = 0.5
    interp_mask = pd.Series(data=np.zeros(len(series_working)),index = series_working.index, name='interp_mask')
    interp_mask.loc[:earliest_filled_index-1] = random.rand(len(series_working.loc[:earliest_filled_index-1]))-0.5

    noisy_interp_data = (series_working + (interp_mask * series_working * noise_scale_factor))
    return noisy_interp_data.rename(series.name)

              
def remove_outliers(series, z_score_threshold = 10):
  """Removes entries from a series which lie more than <threshold> times the standard deviation from the mean. The default should remove more obvious spikes."""
  import scipy.stats
  series = swindon_nhse_data.newCasesByPublishDate
  z_scores = scipy.stats.zscore(series)
  abs_z_scores = np.abs(z_scores)
  return series[(abs_z_scores < z_score_threshold)]              

              
def covid_england_data_blob():
  """Gives you a steaming pile of fresh COVID-19 data from NHSE, Google and Apple"""
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
  df_england_nhse_feed = get_paginated_dataset([f"areaType=nation;areaName=england"], query_structure).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)
  # All UK (some of the metrics only work for the UK as a whole...)
  df_uk_wide_nhse_feed = get_paginated_dataset([f"areaType=overview"], query_structure).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)
  # NHS Regions
  df_nhsregion_nhse_feed = get_paginated_dataset([f"areaType=nhsRegion"], query_structure).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)
  # Regions (geographic regions) - some different metrics than NHS Regions
  df_region_nhse_feed = get_paginated_dataset([f"areaType=region"], query_structure).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)

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
  df_utla_nhse_feed = get_paginated_dataset([f"areaType=utla"], query_structure_2).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)
  # lower tier local authorities (councils and unitary authorities, i.e. Leeds council, Bradford council, York, etc...)
  df_ltla_nhse_feed = get_paginated_dataset([f"areaType=ltla"], query_structure_2).dropna(how='all',axis=1).sort_values(['code','date',]).reset_index(drop=True)

  # google and apple mobility
  df_gb_google_mobility_report = google_mobility().sort_values(["country_region_code","sub_region_1",'date']).reset_index(drop=True)
  df_apple_mobility_report = (apple_mobility().set_index(['geo_type','region','transportation_type','alternative_name','sub-region','country'])
                                                        .rename_axis('date',axis=1).stack()
                                                        .unstack('transportation_type').reset_index())
  df_apple_mobility_report = df_apple_mobility_report[(df_apple_mobility_report.country == "United Kingdom") & 
                                                      ((df_apple_mobility_report['region'] == 'England') 
                                                      | (df_apple_mobility_report['sub-region'] == 'England')
                                                      )]

  return dict(england_nhse=df_england_nhse_feed,
              uk_nhse=df_uk_wide_nhse_feed,
              nhsregion_nhse=df_nhsregion_nhse_feed,
              region_nhse=df_region_nhse_feed,
              utla_nhse=df_utla_nhse_feed,
              ltla_nhse=df_ltla_nhse_feed,
              google_mobility=df_gb_google_mobility_report,
              apple_mobility=df_apple_mobility_report,
              )


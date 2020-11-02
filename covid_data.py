from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get
from http import HTTPStatus
import pandas as pd
import zipfile
import requests
import io
import re

StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType,
                          print_url=False, start_page = 1, end_page=None) -> APIResponseType:
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
          print(previous_url)
          print(f'Reached page {previous_url[-2:]}')
          print(error)
          break
        previous_url = response.url
        if print_url is True:
          print(response.url)
        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']
        
        data.extend(page_data)

        page_number += 1

    return pd.DataFrame(data)
 

def google_mobility(country_filter="_GB_"):
  response = requests.get("https://www.gstatic.com/covid19/mobility/Region_Mobility_Report_CSVs.zip")
  zip_file = zipfile.ZipFile(io.BytesIO(response.content))
  GB_google_mobility_report_paths = list(filter(lambda x: country_filter in x, zip_file.namelist()))
  df_gb_google_mobility_report = pd.concat([pd.read_csv(zip_file.open(file)) for file in GB_google_mobility_report_paths],axis=0)
  return df_gb_google_mobility_report
  

def apple_mobility():
  df_apple_mobility_report = pd.read_csv("https://covid19-static.cdn-apple.com/covid19-mobility-data/2019HotfixDev25/v3/en-us/applemobilitytrends-2020-10-30.csv")
  return df_apple_mobility_report

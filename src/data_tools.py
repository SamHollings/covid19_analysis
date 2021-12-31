import pandas as pd
import statsmodels.tsa.stattools as stattools
import numpy as np


def find_optimum_lags(df: pd.DataFrame) -> int:
  """Submit a dataframe with two columns - this will find the offset needed to aligned them using crosscorrelation"""
  df_ = df.copy().dropna(how='any')
  optimum_lags = np.argmax(stattools.ccf(df_.iloc[:,0].values,df_.iloc[:,1].values))
  print(f"Optimum offset (lag) between {df_.iloc[:,0].name} and {df_.iloc[:,1].name}: {optimum_lags}")
  return optimum_lags


def min_max_scale_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Scales the data frame values between 0 and 1 across the columns allowing for easier comparison of line shape on plots
    :param df: data frame to be scaled
    :return: scaled dataframe
    """
    return df.div(df.max(),axis=1)

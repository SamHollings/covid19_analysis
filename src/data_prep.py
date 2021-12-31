import pandas as pd
import matplotlib.pyplot as plt

#import feature_tools
#import pyspark_tools

#datastore = pyspark_tools.DataStore()

#df = datastore.get_table(table_name="../data/england_nhse_feed.csv")

def get_nhse_feed_local_data():
	"""Gets the local csv files for the NHSE feed and combines them into one pandas dataframe"""
	df = pd.concat([
				pd.read_csv("../data/uk_nhse_feed.csv").tail(100),
				pd.read_csv("../data/england_nhse_feed.csv").tail(100),
				pd.read_csv("../data/region_nhse_feed.csv"),
				#pd.read_csv("../data/nhsregion_nhse_feed.csv").tail(100),
				pd.read_csv("../data/utla_nhse_feed.csv").tail(100),
				pd.read_csv("../data/ltla_nhse_feed.csv").tail(100),
					],
			   axis=0)
	return df

def filter_english_only(df):

	return df[~df['code'].str.contains('[WSN]')]


df = get_nhse_feed_local_data()
df = filter_english_only(df)

df1 = df[df['areatype']=='region'].set_index(['date','name'])[['newCasesByPublishDate','newCasesBySpecimenDate']].unstack(level=['name']).dropna(axis=0).tail(100)

df1.plot()

plt.show()
print()


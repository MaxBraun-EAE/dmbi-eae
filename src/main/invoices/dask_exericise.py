
import pandas as pd
from dask.distributed import Client
import dask.dataframe as dd
from dask.distributed import progress

# Creates Dask clusters
client = Client(n_workers=4, threads_per_worker=2, memory_limit="8GB")
client

# Reads the csv files
consumptions_df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/consumptions_eae.csv')
contracts_df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/contracts_eae.csv')
meteo_df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/meteo_eae.csv', sep=';')
zipcode_df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/zipcode_eae_v2.csv')
profile_df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/profile_eae.csv', sep=';')

# Calculates new columns and formats
consumptions_df['TOTAL_CONSUMPTION'] = consumptions_df['P1'] + consumptions_df['P2'] + consumptions_df['P3']
meteo_df.columns = [x.upper() for x in meteo_df.columns]
zipcode_df.columns = [x.upper() for x in zipcode_df.columns]
zipcode_df = zipcode_df[['PROVINCE', 'ZIPCODE']]

# Creates the consumption dataset and calculates the avg consumption per hour and province
merged_df = consumptions_df.merge(contracts_df, on="CONTRACT_ID").merge(zipcode_df, on="ZIPCODE")
merged_df['key'] = 1
profile_df['key'] = 1
merged_df = merged_df.merge(profile_df, on='key')
merged_df = merged_df.fillna(value=0)
merged_df = merged_df[["PROVINCE", "hour", "TOTAL_CONSUMPTION" ]]
avg_cons_per_hour_province = merged_df.groupby(["PROVINCE", "hour"])["TOTAL_CONSUMPTION"].mean().compute().reset_index()

# Creates the temperature dataset and calculates the avg temperature per hour and province
merged_df2 = meteo_df.merge(zipcode_df, on="ZIPCODE")
merged_df2['key'] = 1
merged_df2 = merged_df2.merge(profile_df, on='key')
merged_df2 = merged_df2.fillna(value=0)
merged_df2 = merged_df2[["PROVINCE", "hour", "TEMPERATURE" ]]
avg_temp_per_hour_province = merged_df2.groupby(["PROVINCE", "hour"])["TEMPERATURE"].mean().compute().reset_index()

# Joins the 2 results in one table
province_h_df = avg_cons_per_hour_province.merge(avg_cons_per_hour_province, on=['PROVINCE', 'hour'])
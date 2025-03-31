import Pandas as pd
import numpy as np 
import dask.dataframe as dd

## Loading Datasets using Pandas
df_distance = pd.read_csv("Trips_By_Distance.csv")
df_full = pd.read_csv("Trips_Full_Data.csv")

## Loading Datasets using Dask
df_distance_dask = dd.read_csv("Trips_By_Distance.csv")
df_full_dask = dd.read_csv("Trips_Full_Data.csv")

## Clean the data using dropna and drop_duplicates to remove inconsistencies 
df_distance.dropna(inplace = True)
df_distance.drop_duplicates(inplace = True)

df_full.dropna(inplace = True)
df_full.drop_duplicates(inplace = True)

## Do the same for the datasets loaded using dask
df_distance_dask.dropna(inplace = True)
df_distance_dask.drop_duplicates(inplace = True)

df_full_dask.dropna(inplace = True)
df_full_dask.drop_duplicates(inplace = True)

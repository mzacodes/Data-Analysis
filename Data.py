import Pandas as pd
import numpy as np 
import dask.dataframe as dd

## Loading Datasets using Pandas
df_distance = pd.read_csv("Trips_By_Distance.csv")
df_full = pd.read_csv("Trips_Full_Data.csv")

## Loading Datasets using Dask
df_distance_dask = dd.read_csv("Trips_By_Distance.csv")
df_full_dask = dd.read_csv("Trips_Full_Data.csv")

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

import pandas as pd
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
df_distance_dask.dropna()
df_distance_dask.drop_duplicates()

df_full_dask.dropna()
df_full_dask.drop_duplicates()

##Q1.a

# Total number of people staying at home
num_staying_home = df_full['Population Staying at Home'].sum()
print(f"Total people staying at home: {num_staying_home}")

# Total number of people NOT staying at home
num_not_home = df_full['People Not Staying at Home'].sum()
print(f"Total people NOT staying at home: {num_not_home}")

# Estimate average trips per person not staying home
avg_trips_per_person = df_full["Trips"].sum() / num_not_home
print(f"Average number of trips per person (not staying home): {avg_trips_per_person:.2f}")

## Using Dask 

num_staying_home_dask = df_full_dask["Population Staying at Home"].sum().compute()
num_not_home_dask = df_full_dask["People Not Staying at Home"].sum().compute()
avg_trips_per_person_dask = (df_full_dask["Trips"].sum() / num_not_home_dask).compute()

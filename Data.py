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

##Q1.b

# Group by Date and sum the trips in each range
trip_counts = df_distance.groupby('Date')[['Number of Trips 10-25', 'Number of Trips 50-100']].sum()

# Find dates where more than 10M people made 10-25 trips
dates_10_25 = trip_counts[trip_counts['Number of Trips 10-25'] > 10_000_000].index

# Find dates where more than 10M people made 50-100 trips
dates_50_100 = trip_counts[trip_counts['Number of Trips 50-100'] > 10_000_000].index

print("Dates where > 10 Million people made 10-25 trips:", list(dates_10_25))
print("Dates where > 10 Million people made 50-100 trips:", list(dates_50_100))

## Using Dask

# Sum trips in Dask 
trip_counts_dask = df_distance_dask.groupby("Date")[["Number of Trips 10-25", "Number of Trips 50-100"]].sum().compute()

# Filter dates for greater than 10 Million trips
dates_10_25_dask = trip_counts_dask[trip_counts_dask["Number of Trips 10-25"] > 10_000_000].index
dates_50_100_dask = trip_counts_dask[trip_counts_dask["Number of Trips 50-100"] > 10_000_000].index

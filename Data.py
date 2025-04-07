import pandas as pd
import numpy as np 
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import time
import matplotlib.pyplot as plt
import seaborn as sns

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

##Q1.c

# Function to measure execution time
def measure_runtime(n_workers):
  cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
  client = Client(cluster)
    
  start_time = time.time()

  # Dask query
  df_distance_dask.groupby("Date")[["Number of Trips 10-25", "Number of Trips 50-100"]].sum().compute()

  end_time = time.time()
  runtime = end_time - start_time
    
  client.close()
    
  return runtime

# Measure time with 10 and 20 workers
time_10_workers = measure_runtime(10)
time_20_workers = measure_runtime(20)


##Q1.d 

trip_cols = [
    'Number of Trips <1', 'Number of Trips 1-3', 'Number of Trips 3-5',
    'Number of Trips 5-10', 'Number of Trips 10-25', 'Number of Trips 25-50',
    'Number of Trips 50-100', 'Number of Trips 100-250', 
    'Number of Trips 250-500', 'Number of Trips >=500'
]

# Sum total number of trips across all categories
trip_totals = df_distance[trip_cols].sum()
trip_distribution = trip_totals / trip_totals.sum() 

print("Trip probability distribution:\n", trip_distribution)

simulated_travel = np.random.choice(trip_distribution.index, p=trip_distribution.values, size=100000)

# Convert to a DataFrame and count frequencies
simulated_df = pd.DataFrame(simulated_travel, columns=['Trip_Length'])
sim_counts = simulated_df['Trip_Length'].value_counts().sort_index()

print("\nSimulated travel frequencies:\n", sim_counts)

##Q1.e

# Sum each distance-trip bin across all dates
trip_totals_sorted = trip_totals.sort_values(ascending=False)

plt.figure(figsize=(12, 6))
sns.barplot(x=trip_totals_sorted.index, y=trip_totals_sorted.values, palette="viridis")
plt.xticks(rotation=45)
plt.title("Number of Trips by Distance Category")
plt.ylabel("Number of Trips")
plt.xlabel("Trip Distance Category")
plt.tight_layout()
plt.show()

## The graph shows that:
##                       -Short distance trips are most frequent 
##                       -Long distance trips are rare
##                       -it aligns with historical trends, thus validating its assumptions
##                       -the model can forecast transportation demand


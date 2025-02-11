
import pandas as pd
from dask.distributed import Client
import dask.dataframe as dd
from dask.distributed import progress

client = Client(n_workers=2, threads_per_worker=2, memory_limit="1GB")
client


df = dd.read_csv('C:/Users/roger.lloret/Documents/EAE/assignment_kettle/consumptions_eae.csv').persist()
progress(df)

df.P2.sum().compute()
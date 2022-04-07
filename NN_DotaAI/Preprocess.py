import JsonToCsv
import torch
import numpy as np
import pandas as pd


# Todo
# Data we want to add:
# Distance to friendly heroes
# Distance to enemy heroes
# Distance to closest friendly tower
# Distance to closest enemy tower
# Range for hero_id

# Data Preprocessing:
# Cleaning -> Remove pause, Already removed: Picking phase.
# Multiple datasets
# Normalize, -> 0, 1. (gradient)
# Classification label, i.e. output. Time to die uses death.

# Open folder with .json files, convert each to csv,
# alter file according to above, add dist etc.
# store altered .csv file in processed directory

# Store tower positions
tower_positions = {"r1": 1,
                   "r2": 2,
                   "r3": 3,
                   "d1": 1,
                   "d2": 2,
                   "d3": 3}

def add_fields(dataframe):
    dataframe["enemy proximity"] = 1
    dataframe["ally proximity"] = 1
    dataframe["closest ally tower"] = 1
    dataframe["closest enemy tower"] = 1
    # Todo: Also add proximity changes based on time t


def process():
    JsonToCsv.convert('test.json')
    with open('test.json.csv') as f:
        pd_df = pd.read_csv('test.json.csv')

    add_fields(dataframe=pd_df)
    print(pd_df)

    # Create tensor, pandas to_numpy()
    tensor = torch.tensor(pd_df.to_numpy(), dtype=float)
    print(tensor)


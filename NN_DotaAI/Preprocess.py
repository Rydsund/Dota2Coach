import JsonToCsv
import torch
import numpy as np
import pandas as pd


def process():
    JsonToCsv.convert('test.json')
    with open('test.json.csv') as f:
        pd_df = pd.read_csv('test.json.csv')
        np_df = np.genfromtxt('test.json.csv', delimiter=',', skip_header=1, dtype=float)

    # Skapa tensor, pandas to_numpy()
    tensor = torch.tensor(pd_df.to_numpy(), dtype=float)
    print(tensor)

    ## Todo
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



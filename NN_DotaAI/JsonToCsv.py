import pandas as pd


def convert(path):
    df = pd.read_json(path, lines=True)
    df.to_csv(path + '.csv')

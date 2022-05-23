import concurrent.futures
import math
import os
import numpy as np
import pandas as pd
from threading import current_thread

# Config
parent_dir = os.getcwd()
PATH_TO_JSON = os.path.join(parent_dir, "jsonfiles")
PATH_TO_CSV = os.path.join(parent_dir, "csvfiles")

# Some variables are unobtainable from the parser. Define and implement them in this class.

# Global variables for patch 7.31. Remember that these are subject to change in each Dota 2 patch.
# Hard coded outer tower positions:
radiant_bot_t1 = [4913, -6161]
radiant_mid_t1 = [-1538, -1413]
radiant_top_t1 = [-6277, 1821]

dire_top_t1 = [-4653, 6000]
dire_mid_t1 = [521, 677]
dire_bot_t1 = [6259, -16992]

towers_radiant = [radiant_top_t1, radiant_mid_t1, radiant_bot_t1]
towers_dire = [dire_top_t1, dire_mid_t1, dire_bot_t1]


# Calculate the distance to the closest allied tower
# In this case, only the first tower of respective lane.
def calc_distance_tower_ally(x0, y0, team):
    min_dist = math.inf
    if team == 0:   # Team Radiant
        for pos in towers_radiant:
            x1 = pos[0]
            y1 = pos[1]
            dist = math.sqrt((x1 - x0)**2 + (y1 - y0)**2)
            if dist < min_dist:
                min_dist = dist
        return min_dist

    if team == 1:   # Team Dire
        for pos in towers_dire:
            x1 = pos[0]
            y1 = pos[1]
            dist = math.sqrt((x1 - x0) ** 2 + (y1 - y0) ** 2)
            if dist < min_dist:
                min_dist = dist
        return min_dist


# Calculate the distance to the closest enemy tower
# In this case, only the first tower of respective lane.
def calc_distance_tower_enemy(x0, y0, team):
    min_dist = math.inf
    if team == 1:  # Team Dire
        for pos in towers_radiant:
            x1 = pos[0]
            y1 = pos[1]
            dist = math.sqrt((x1 - x0)**2 + (y1 - y0)**2)
            if dist < min_dist:
                min_dist = dist
        return min_dist

    if team == 0:
        for pos in towers_dire:
            x1 = pos[0]
            y1 = pos[1]
            dist = math.sqrt((x1 - x0) ** 2 + (y1 - y0) ** 2)
            if dist < min_dist:
                min_dist = dist
        return min_dist


# First five heroes/players are on team radiant
def calc_team(index):
    value = index % 10
    for i in range(0, 5):
        if value == i:
            return 0
    return 1

# Find and add the closest ally hero to dataframe - df
def calc_closest_ally_hero(x0, y0, team, time, df):
    result = df.loc[(df['time'] == time) & (df['team'] == team) & (df['x'] != x0) & (df['y'] != y0)]
    nr_of_rows = result.shape[0]
    min_dist = math.inf
    for i in range(0, nr_of_rows):
        x1 = result['x'].iloc[i]
        y1 = result['y'].iloc[i]
        dist = math.sqrt((x1 - x0) ** 2 + (y1 - y0) ** 2)
        if dist < min_dist:
            min_dist = dist
    return min_dist


# Find and add the closest enemy hero to dataframe - df
def calc_closest_enemy_hero(x0, y0, team, time, df):
    team = 1 - team  # Invert team selection
    result = df.loc[(df['time'] == time) & (df['team'] == team) & (df['x'] != x0) & (df['y'] != y0)]
    nr_of_rows = result.shape[0]
    min_dist = math.inf
    for i in range(0, nr_of_rows):
        x1 = result['x'].iloc[i]
        y1 = result['y'].iloc[i]
        dist = math.sqrt((x1 - x0) ** 2 + (y1 - y0) ** 2)
        if dist < min_dist:
            min_dist = dist
    return min_dist


# Add columns to dataset.
def preprocess(df):
    # Assign team based on index in file.
    df['team'] = df.apply(lambda row: calc_team(row.name), axis=1)

    # Find the closest ally and enemy tower for each time t.
    # Expensive due to the fact it has to be calculated for each hero i.e. needs 5x dataframe
    df['closest_ally_tower_distance'] = \
        df.apply(lambda row: calc_distance_tower_ally(row['x'], row['y'], row['team']), axis=1)
    df['closest_enemy_tower_distance'] = \
        df.apply(lambda row: calc_distance_tower_enemy(row['x'], row['y'], row['team']), axis=1)

    # Find the closest ally and enemy hero for each time t.
    # Expensive due to the fact it has to be calculated for each hero i.e. needs 5x dataframe
    df['closest_ally_hero_distance'] = \
        df.apply(lambda row: calc_closest_ally_hero(row['x'], row['y'], row['team'], row['time'], df), axis=1)
    df['closest_enemy_hero_distance'] = \
        df.apply(lambda row: calc_closest_enemy_hero(row['x'], row['y'], row['team'], row['time'], df), axis=1)


# Saves each replay as its own csv file.
def run_multiple():
    # Boolean to toggle headers on and off:
    use_headers = True
    for file in os.listdir(PATH_TO_JSON):
        if '.json' in file:
            existing_file_path = os.path.join(PATH_TO_JSON, file).replace('\\', '/')
            try:
                df = pd.read_json(existing_file_path, lines=True)  # line delimited
            except(BaseException, OSError, TypeError):
                print("Not able to read from: " + existing_file_path)
                continue

            # Edit current file:
            preprocess(df)

            # Save file as new .csv file:
            new_file_name = file.replace('.json', '.csv')
            new_file_path = os.path.join(PATH_TO_CSV, new_file_name).replace('\\', '/')
            try:
                df.to_csv(new_file_path, header=use_headers, index=False)
            except(BaseException, OSError, TypeError):
                print("Something went wrong at " + new_file_path + " when trying to save as CSV.")


# Appends all replays to dataset.csv
def run_single():
    # Boolean to toggle headers on and off:
    use_headers = True
    for file in os.listdir(PATH_TO_JSON):
        if '.json' in file:
            existing_file_path = os.path.join(PATH_TO_JSON, file).replace('\\', '/')
            try:
                df = pd.read_json(existing_file_path, lines=True)  # line delimited
            except(BaseException, OSError, TypeError):
                print("Not able to read from: " + existing_file_path)
                continue

            # Edit current file:
            preprocess(df)

            # Append dataframe to dataset.csv:
            new_file_path = os.path.join(PATH_TO_CSV, 'dataset.csv').replace('\\', '/')
            if not os.path.exists(new_file_path):
                # Add headers if use_headers == True and file does not exist.
                headers = use_headers
            try:
                # Removes headers and index, appends to file.
                df.to_csv(new_file_path, header=headers, index=False, mode='a')
                if headers:
                    # Only add headers once.
                    headers = False
            except(BaseException, OSError, TypeError):
                print("Something went wrong at " + new_file_path + " when trying to save as CSV.")


def run_concurrent_per_thread():
    # max_workers specifies how many threads to run. None means no limit i.e. maximum available threads.
    with concurrent.futures.ThreadPoolExecutor(max_workers=None) as executor:
        files = os.listdir(PATH_TO_JSON)
        future_csv = {executor.submit(read_json_write_csv_per_thread, df): df for df in files}
        for future in concurrent.futures.as_completed(future_csv):
            df = future_csv[future]
            executor.map(df, range(len(files)))


def read_json_write_csv_per_thread(file):
    # Boolean to toggle headers on and off:
    use_headers: bool = True
    if '.json' in file:
        existing_file_path = os.path.join(PATH_TO_JSON, file).replace('\\', '/')
        try:
            df = pd.read_json(existing_file_path, lines=True)  # line delimited
        except(BaseException, OSError, TypeError):
            print("Not able to read from: " + existing_file_path)

    preprocess(df)

    # Append dataframe to 'thread_id'_dataset.csv:
    # Will create as many csv files as there are threads running.
    new_file_name = str(current_thread().ident) + "_dataset.csv"

    new_file_path = os.path.join(PATH_TO_CSV, new_file_name).replace('\\', '/')
    if not os.path.exists(new_file_path):
        # Add headers if use_headers == True and file does not exist.
        headers = use_headers
    else:
        # Only add headers at the top of the csv file
        headers = False
    try:
        # Removes headers and index, appends to file.
        df.to_csv(new_file_path, header=headers, index=False, mode='a')
    except(BaseException, OSError, TypeError):
        print("Something went wrong at " + new_file_path + " when trying to save as CSV.")



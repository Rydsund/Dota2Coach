import os
import bz2
import subprocess
import preprocess
import requests
from urllib import request as req
import csv
import concurrent.futures

# Config
# os.cwd() fetches Current Working Directory.
parent_dir = os.getcwd()
# Replace 'file_name.csv' below with the name of the csv-file containing the match ids you want to download.
# Try a smaller batch of ids first.
file_name = "file_name.csv"
PATH_TO_MATCH_IDS = os.path.join(parent_dir, file_name)
# Creates a folders, for each step of the process, in the directory, if no such folder exists.
PATH_TO_BZ2 = os.path.join(parent_dir, "BZ2")
if not os.path.isdir(PATH_TO_BZ2):
    os.mkdir(PATH_TO_BZ2)
PATH_TO_DEM = os.path.join(parent_dir, "demfiles")
if not os.path.isdir(PATH_TO_DEM):
    os.mkdir(PATH_TO_DEM)
PATH_TO_JSON = os.path.join(parent_dir, "jsonfiles")
if not os.path.isdir(PATH_TO_JSON):
    os.mkdir(PATH_TO_JSON)
PATH_TO_CSV = os.path.join(parent_dir, "csvfiles")
if not os.path.isdir(PATH_TO_CSV):
    os.mkdir(PATH_TO_CSV)
# Replace "git-bash" with full path to your git-bash.exe or equivalent
PATH_TO_GIT_BASH = "git-bash.exe"
# Replace 'your_key' below with your api_key.
# You can get a key from https://www.opendota.com/api-keys.
api_key = "your_key"
a_url = "https://api.opendota.com/api/replays"

def get_responses():
    # You can get match ids from opendota.com/explorer. Toggle SQL to modify query. Example can be found in README.md.
    match_ids = []
    with open(PATH_TO_MATCH_IDS, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        for row in reader:
            match_ids.append(row)

    # Add the urls, for all valid match ids, to a list.
    responses = []
    for i in match_ids:
        params = {'match_id': i, 'api_key': api_key}
        try:
            response = requests.get(a_url, params=params)
            j_res = response.json()
            responses.append(j_res)
        except (TypeError, BaseException):
            print(params + " invalid match id. Replay not downloaded")
            continue

    return responses


def download(file):
    # This is how the url for downloading a replay is composed:
    # replay"cluster".valve.net/570/"match_id"_"replay_salt".dem.bz2
    try:
        # Extract 'match-id', 'cluster' and 'replay_salt' from responses and concatenate them to a valid url.
        match_id = file[0]["match_id"]
        cluster = file[0]["cluster"]
        salt = file[0]["replay_salt"]
        v_url = \
            "http://replay" + str(cluster) + ".valve.net/570/" + str(match_id) + "_" + str(salt) + ".dem.bz2"
    except(TypeError, BaseException):
        print("url incorrect. Replay not downloaded for match id: " + match_id)

    try:
        local_file = os.path.join(PATH_TO_BZ2, str(match_id) + ".dem.bz2")
        download_replay = req.urlretrieve(v_url, local_file)
    except(OSError, TypeError, BaseException):
        print("Could not retrieve replay. Replay not downloaded for match_id: " + match_id)


# For extracting .dem files from PATH_TO_BZ2, store in PATH_TO_DEM
def extract(file):
    if ".bz2" in file:
        existing_file_path = os.path.join(PATH_TO_BZ2, file)
        print(existing_file_path)
        try:
            with bz2.open(existing_file_path, 'rb') as f:
                uncompressed_content = f.read()
        except(BaseException, OSError, TypeError):
            print("Could not open path: " + existing_file_path)

        new_file_name = file.replace('.bz2', '')
        new_file_path = os.path.join(PATH_TO_DEM, new_file_name)
        try:
            with open(new_file_path, 'wb') as f:
                f.write(uncompressed_content)
                f.close()
        except(BaseException, OSError, TypeError):
            print("Could not open or write to path: " + new_file_path)


# For parsing .dem files via the modified open dota parser.
# Needs bash installed, we use git bash.
def parse(file):
    if '.dem' in file:
        existing_file_path = os.path.join(PATH_TO_DEM, file)
        new_file_name = file.replace('.dem', '.json')
        new_file_path = os.path.join(PATH_TO_JSON, new_file_name).replace('\\', '/')

        try:
            bash_command = 'curl -o ' + new_file_path + ' localhost:5600 --data-binary ' + '"@' + existing_file_path + '"'
            p = subprocess.Popen([PATH_TO_GIT_BASH, '-c', bash_command])
            p.wait()
        except(BaseException, OSError, TypeError):
            print("Not able to run Bash command: " + bash_command)


# Thread pool for concurrency, highly recommended for parse() and extract()
# Recommended for download() and preprocess if writing to multiple disks,
# otherwise disk write speed could be a bottle neck.
# thread_pool takes three argument: * the function to run i.e. download, extract or parse.
#                                   * path of files to be processed
#                                   * number of workers/threads. if set to None it uses max available threads.
def thread_pool(func, path, workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        if path == PATH_TO_MATCH_IDS:
            files = get_responses()
        else:
            files = os.listdir(path)
        future_file = {executor.submit(func, file): file for file in files}
        for future in concurrent.futures.as_completed(future_file):
            file = future_file[future]
            executor.map(file, range(len(files)))


def main():
    # NOTE: When dealing with large amounts of replays it is recommended execute one step at the time due to the fact
    # that all steps can take som time to perform, depending on your hardware and internet connection.

    # Step 1: Download .dem files from Valve's servers using OpenDota's api.
    print("Download begin...")
    #thread_pool(download, PATH_TO_MATCH_IDS, None)
    print("Download end.")

    # Step 2: Extract .dem files from .bz2 files in archive
    print("Extract begin...")
    #thread_pool(extract, PATH_TO_BZ2, None)
    print("Extract end.")

    # Step 3: Feed .dem files to parser
    # Start parser, by running main.java in Eclipse(or another compatible IDE)
    print("Parse begin...")
    #thread_pool(parse, PATH_TO_DEM, None)
    print("Parse end.")

    # Step 4: Edit .json files and add attributes, preprocessing.
    # see preprocess.py for alternative methods of preprocessing.
    # implement your own calculations in preprocess.py and modify the script to fit your dataset's needs.
    print("Preprocess begin...")

    # run_single - preprocess matches and append them all to a single file called dataset.csv
    #preprocess.run_single()                 # Recommended

    # run_multiple - preprocess each match and save them as 'match_id'.csv
    # preprocess.run_multiple()                # Recommended

    # Use concurrent_per_thread if write speed is not a limitation. Probably only when writing to more than one disk.
    preprocess.run_concurrent_per_thread()
    print("Preprocess end.")


if __name__ == "__main__":
    main()


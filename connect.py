import requests
import json
import re

from time import sleep
from random import random
from helpers import connect_db


def wait(delay=4, variation=2):
    m, x, c = variation, random(), delay - variation / 2
    sleep(m * x + c)

SITE = "https://understat.com/match/"
HEADERS = {"User-Agent": "nraitanen@gmail.com"}


def get_match_data(match_id):
    # Pull data by GET requesting from Understat website
    r = requests.get(SITE + str(match_id), headers=HEADERS)
    
    if r.status_code != 200:
        print("Match id " + str(match_id) + " not found.")
        wait()
        return False
    
    print("Fetching data, match id: " + str(match_id) + "...")
    
    # Select the JSON strings by regex and clean it up a bit
    shotdata = re.search(r'shotsData\s+=\s+(.*)', r.text).group(0)
    shotdata = json.loads(re.search('\(\'(.*?)\'\)', shotdata).group(1).\
                          encode("utf-8").decode("unicode_escape"))
           
    matchinfo = re.search(r'match_info\s+=\s+(.*)', r.text).group(1)
    matchinfo = json.loads(re.search('\(\'(.*?)\'\)', matchinfo).group(1).\
                           encode("utf-8").decode("unicode_escape"))

    rosterdata = re.search(r'rostersData\s+=\s+(.*)', r.text).group(1)
    rosterdata = json.loads(re.search('\(\'(.*?)\'\)', rosterdata).group(1).\
                            encode("utf-8").decode("unicode_escape"))
    
    match_data = {"match_id": match_id,
                  "shots": shotdata,
                  "matchinfo": matchinfo,
                  "rosters": rosterdata}
    
    # Slow down to reduce load on server    
    wait()
    print("Completed.")
    
    return(match_data)


def push_match_data(match_data, db_name="footie", collection="matches",
                    overwrite=False):
    # Open up connection to MongoClient and select db/collection
    client, con = connect_db(db_name, collection)
    
    # Check if record already exists
    if bool(con.find_one({"match_id": match_data["match_id"]})):
        if overwrite:
            con.replace_one({"match_id": match_data["match_id"]},
                             match_data)
        else:
            print("Match already exists in database - no overwrite selected!")
    
    # If no previous records, just insert a new one
    else:
        # Insert match record to selected collection
        con.insert_one(match_data)
        print("Match data successfully inserted to collection "" + \
              collection + "".")
    # Close the connection
    client.close()
        
    
for i in range(2614, 15000):
    mdata = get_match_data(i)
    if mdata:
        push_match_data(mdata)
        
        
# LAST: 13228
    
    
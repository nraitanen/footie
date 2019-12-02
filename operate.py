from pandas.io.json import json_normalize
from helpers import connect_db

def list_league_ids(db_name="footie", collection="matches"):
    # Connect to db
    client, con = connect_db(db_name, collection)
    
    # Combine league IDs with respective names
    z = list(zip(con.distinct("matchinfo.league_id"),
                 con.distinct("matchinfo.league")))
    
    # Close the connection afterwards
    client.close()
    
    return z
    
def query_league_shots(league_id, db_name="footie", collection="matches"):
    # Connect to db
    client, con = connect_db(db_name, collection)
    
    # Get cursor to point all matches with selected league id
    cursor = con.find( { "matchinfo.league_id": str(league_id) } )
    
    # Loop through shot data on selected matches and append holder list
    # while doing it, then normalize from JSON to pandas DataFrame
    holder = []
    for doc in list(cursor):
        for obj in doc["shots"]["h"]:
            holder.append(obj)
        for obj in doc["shots"]["a"]:
            holder.append(obj)
    df = json_normalize(holder).sort_values("date")
    
    # Close the client
    client.close()
    
    # Return as a pandas DataFrame
    return df

def query_all_players(db_name="footie", collection="matches"):
    # Connect to db
    client, con = connect_db(db_name, collection)
    
    holder = []
    for doc in list(con.find( {} )):
        for obj in doc["rosters"]["h"]:
            holder.append(doc["rosters"]["h"][obj])
            
            # Add date!!
    
    df = json_normalize(holder)
    
    # Close the client
    client.close()
    
    # Return as a pandas DataFrame
    return df
    
    

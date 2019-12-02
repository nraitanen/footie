from pymongo import MongoClient

def connect_db(db_name, collection):
    # Open up connection to MongoClient and select db/collection
    try:
        client = MongoClient(port=27017)
    except:
        print("Couldn't connect to a MongoDB server.")
        
    con = client[db_name][collection]
    
    return client, con
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 21:16:41 2015

@author: Tony
"""

"""
file to load the json document created by map_cleaner.py, and run some MongoDB
queries against the albuquerque map
"""

import json
import pprint

JFILE = "C:/Users/Tony/Documents/udacity/DataScience/Data_Wrangling/open_street_map/albuquerque_new-mexico.osm.json"
#JFILE = "C:/Users/amaleric/Documents/data_science/project2/albuquerque_new-mexico.osm.json"
#JFILE = "C:/Users/Tony/Documents/udacity/DataScience/Data_Wrangling/open_street_map/abqMap_smaller.json"


def query_abq_map(load = False):
    print("running query function")
    
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client.abqMap
    print("made connection to mongodb")
    
    if load:
        # Now load the json to MongoDB
        with open(JFILE) as f:
            data = json.loads(f.read())
            print("loaded json map file")
            db.abqMap.insert(data)
            
        
        # close json file
        f.close
    
    pprint.pprint(db.abqMap.find_one())
    
#    pipeline = [ { "$group" : { "_id" : "$source", "count" : { "$sum" : 1 }}},
#                 { "$sort" : { "count" : -1 }} ]
#    result = db.abqMap.aggregate(pipeline)
#    pprint.pprint(result)
    
#    query = {"foundingDate" : {"$gte" : datetime(2001,1,1)} }
#    cities = db.abqMap.find(query)
#
#    print "Found cities:", cities.count()
#    import pprint
#    pprint.pprint(cities[0])
    
    # Number of documents
    num_docs = db.abqMap.find().count()
    print("Number of documents: " + str(num_docs))

    # Number of nodes
    num_nodes = db.abqMap.find({"type":"node"}).count()
    print("Number of nodes: " + str(num_nodes))

    # Number of ways
    num_ways = db.abqMap.find({"type":"way"}).count()
    print("Number of ways: " + str(num_ways))

    # search for the types of amenities
    pipeline = [
                 { "$match": {"amenity":{"$exists":1}}},
                 { "$group": {"_id":"$amenity", "count":{"$sum":1}}},
                 { "$sort" : { "count" : -1 } }, 
                 { "$limit": 10}
               ]
    result = db.abqMap.aggregate(pipeline)
    print('Top amenities:')
    pprint.pprint(result["result"])

    # search the number of restaurants on central ave
    pipeline = [
                 { "$match": {"amenity": "restaurant" } },
                 { "$group": {"_id":"$cuisine", "count" : {"$sum" : 1} } },
                 { "$sort" : { "count" : -1 } }, 
                 { "$limit": 10}
               ]
    result = db.abqMap.aggregate(pipeline)
    print('Top cuisines:')
    pprint.pprint(result["result"])

    # Number of unique users
#    num_users = db.abqMap.distinct({"created.user"}).length
#    print("Number of unique users: " + str(num_users))

    # Top 1 contributing user
#    top_user = db.abqMap.aggregate( [{"$group":{"_id":"$created.user", "count":{"$sum":1}}}, {"sort":{"count":­1}, {"$limit":1}  ]  )
#    print("Top user: " + top_user)

    # Number of users appearing only once (having 1 post)
    #db.abqMap.aggregate([{"$group":{"_id":"$created.user", "count":{"$sum":1}}}, {"$group":{"_id":"$count", "num_users":{"$sum":1}}}, {"$sort":{"_id":1}}, {"$limit":1}])
        
    
    # close the database
    db.close


if __name__ == "__main__":

    query_abq_map()
    
    
    
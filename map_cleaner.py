# -*- coding: utf-8 -*-
"""
Created on Sat Apr 11 14:55:49 2015

@author: Tony
"""

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET
import pprint
import re
import codecs
import json

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
ABQ_OSM = "C:/Users/Tony/Documents/udacity/DataScience/Data_Wrangling/open_street_map/albuquerque_new-mexico.osm"
#ABQ_OSM = "C:/Users/amaleric/Documents/data_science/project2/albuquerque_new-mexico.osm"
MAPPING = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "avenue": "Avenue",
            "Rd.": "Road",
            "Rd": "Road",
            "Dr": "Drive",
            "Pl": "Place",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "BLVD": "Boulevard",
            "Norhteast": "Northeast",
            "SE": "Southeast",
            "NE": "Northeast",
            "NE.": "Northeast",
            "SW": "Southwest",
            "NW": "Northwest",
            "SouthWest": "Southwest"
            }

"""
count_tags uses iterative parsing to process the map file and find out what
and how many tags there are
"""
def count_tags(elem, tags):
    if elem.tag in tags:
        tags[elem.tag] += 1
    else:
        tags[elem.tag] = 1
            
    return tags
    
    
"""
check_tags is from the MongoDB course tags.py
It checks the k-value of any tag from the map data and compares it to some
regular expressions to see if they can be valid keys in MongoDB
"""
def check_tags(element, keys):
    if element.tag == "tag":
        kvalue = element.attrib['k']
        #print(kvalue)
        
        if lower.search(kvalue):
            keys["lower"] += 1
        elif lower_colon.search(kvalue):
            keys["lower_colon"] += 1
        elif problemchars.search(kvalue):
            keys["problemchars"] += 1
        else:
            keys["other"] += 1

    return keys
    
    
"""
find_users is from the MongoDB course users.py
It returns a set with the individual contributors to the map data
"""
def find_users(element, users):
    if 'user' in element.attrib:
        users.add(element.attrib['user'])

    return users
    

"""
cleans and standardizes the street names
"""
def update_street_name(name):

    name_parts = name.split()
    
    for k in MAPPING:
        # The street or section designation could be either last or second to last
        if name_parts[-1].endswith(k):
            name_parts[-1] = name_parts[-1].replace(k, MAPPING[k])
        if (len(name_parts) > 1):
            if name_parts[-2].endswith(k):
                name_parts[-2] = name_parts[-2].replace(k, MAPPING[k])

    name = " ".join(name_parts)
    return name


"""
shape_element the function we created in the MongoDB course, in data.py
It takes the osm element and returns a dictionary formatted to our liking
"""
def shape_element(element):
    node = {}
    node['pos'] = [0.0, 0.0]
    created = {}

    node['type'] = element.tag
    for a in element.attrib:
        if a in CREATED:
            created[a] = element.attrib[a]
        elif a == 'lat':
            node['pos'][0] = float(element.attrib[a])
        elif a == 'lon':
            node['pos'][1] = float(element.attrib[a])
        else:
            node[a] = element.attrib[a]
        
        # process the second level tags
        addr = {}
        for tag in element.iter("tag"):
            ktype = tag.attrib["k"]
            kvalue = tag.attrib["v"]
            if ktype.startswith("addr:"):
                if len(ktype.split(":")) > 2:
                    # odd format, two ":"'s, punt
                    pass
                else:
                    new_key = ktype.split(":")[1]
                    
                    # Want to clean and standardize the street types
                    if (new_key == "street"):
                        #orig_name = kvalue
                        kvalue = update_street_name(kvalue)
                        #if (orig_name != kvalue):
                        #    print(orig_name + " --> " + kvalue)
                        
                    addr[new_key] = kvalue
            else:
                node[ktype] = kvalue
            
        if len(addr) > 0:
            node['address'] = addr
                 
        # Handle the node refs in tag type "way"
        if element.tag == "way":
            nd_refs = []
            for nd in element.iter("nd"):
                if "ref" in nd.keys():
                   nd_refs.append(nd.get("ref"))
                   
            if len(nd_refs) > 0:
                node["node_refs"] = nd_refs
            
    node['created'] = created

    return node


def map_cleaner(file_in):
    
    """ Run the file iteration here, and not in the individual functions - then
        we loop only once instead of multiple times - more efficient """
    
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    tags = {}
    
    # use a set for the users, since a set does not have duplicates
    users = set()

    file_out = "{0}.json".format(file_in)
    data = []
    count = 0

    with codecs.open(file_out, "w") as fo:
        # Write an opening bracket for the json file
        fo.write('[\n')
        
        # Iterate through all the elements in the .osm data
        for _, element in ET.iterparse(file_in):
            """ prepare a dictionary of the type of tags and how many """
            tags = count_tags(element, tags)
            
            """ audit the k-values for key validity """
            keys = check_tags(element, keys)
            
            """ maintain the set of unique users """
            users = find_users(element, users)
            
            """ add types node and way to data, while shaping to our desired format """
            if element.tag == "node" or element.tag == "way" :
                el = shape_element(element)
                data.append(el)
                count += 1
                if count > 1:
                    fo.write(",\n")
                fo.write(json.dumps(el))
                
                # Display progress, so I know the program didn't hang
                if (count % 10000 == 0):
                    print("Wrote " + str(count) + " entries")
                    
        # Closing bracket for the json file
        fo.write('\n]\n')
            
    # Display total number of entries written
    print('NUMBER OF ENTRIES WRITTEN TO JSON:')
    print(count)
    
    # Display what tags are present
    print('TAGS PRESENT IN THE MAP DATA:')
    pprint.pprint(tags)
    
    # Display audit results of the tags' k-value
    print('\nRESULT FROM TAG KEY AUDIT:')
    pprint.pprint(keys)

    # Display number of unique contributors
    print('\nNUMBER OF CONTRIBUTORS TO THE MAP DATA:')
    print(len(users))
    #pprint.pprint(users)
    
    # Shape the data and output to json format
    print('\nHERE ARE THE FIRST AND LAST FORMATTED ENTRIES:')
    pprint.pprint(data[0])
    pprint.pprint(data[-1])

    

if __name__ == "__main__":
    map_cleaner(ABQ_OSM)
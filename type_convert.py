"""
mongo import converts ints to strings.
this script will convert the object types in mongo collections
"""

import pymongo
import logging
from datetime import datetime

client = pymongo.MongoClient()
db = client.ufo

logging.basicConfig(filename='log_type_convert.log',
                    filemode='w+',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("Begin Script")

# convert all landmass and population values to int
state_objects = db.states.find({}, {"sightings":0})
for state_obj in state_objects:
    population = state_obj.get("population")
    try:
        int_pop = population.replace(",","")
    except:
        int_pop = population
    int_pop = int(int_pop)

    landmass = state_obj.get("landmass")
    try:
        int_landmass = landmass.replace(",","")
    except:
        int_landmass = landmass
    int_landmass = int(int_landmass)

    state = state_obj.get("state")

    logging.info("replacing {} with {}".format(population, int_pop))
    db.states.update({"state": state}, {"$set": {"population": int_pop, "landmass":int_landmass}}) 

# convert all sightings datetime string to JSON
all_sightings = db.sightings.find()
for sighting in all_sightings:
    og_datetime = sighting.get("datetime")
    strfmt = "%m/%d/%Y %H:%M"
    try:
        # Error occurs when dealing with a sighting from Rome that had an hour value of 936048
        new_datetime = datetime.strptime(og_datetime, strfmt)
    except:
        new_datetime = None

    logging.info("replacing {} with {}".format(population, int_pop))
    db.sightings.update({"_id": sighting.get("_id")}, {"$set": {"datetime": new_datetime}}) 

logging.info("success")

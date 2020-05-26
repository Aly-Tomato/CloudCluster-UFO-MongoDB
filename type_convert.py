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
for state_obj in db.states.find({}, {"sightings":0, "_id":0}):
    population = state_obj.get("population")
    int_pop = int(population.replace(",","")

    landmass = state_obj.get("landmass")
    int_landmass = int(landmass.replace(",",""))

    state = state_obj.get("state")

    logging.info("replacing {} with {}".format(population, int_pop))
    db.states.update({"state": state}, {"$set": {"population": int_pop, "landmass":int_landmass}}) 

# convert all sightings datetime string to JSON
for sighting in db.sightings.find():
    og_datetime = sightings.get("datetime")
    strfmt = "%m/%d/%Y %H:%M"
    new_datetime = datetime.strptime(og_datetime, strfmt)

    logging.info("replacing {} with {}".format(population, int_pop))
    db.sightings.update({"_id": sightings.get("_id")}, {"$set": {"datetime": new_datetime}}) 

logging.info("success")

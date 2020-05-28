import pymongo
import logging

client = pymongo.MongoClient()
db = client.ufo

logging.basicConfig(filename='log_wINDX_embed.log',
                    filemode='w+',
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info("Begin Script")

for state_obj in db.states.find({}, {"state":1, "_id":0}):
    state = state_obj.get("state")

    sightings_list = []
    
    #naive set 
    state_sightings = db.sightings.find({"state":state})
    for s in state_sightings:
        sightings_list = [x for x in state_sightings]
    
    logging.info("updating {} with {} total sightings".format(state, len(sightings_list)))
    db.states.update({"state": state}, {"$set": {"Number of sightings": len(sightings_list)}})
    db.states.update({"state": state}, {"$set": {"sightings": sightings_list}})

logging.info("success")

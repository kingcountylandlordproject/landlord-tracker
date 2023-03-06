#!/bin/bash

cd /landlord-tracker/aleph_lt

# pipe JSON output to "ftm store" to aggregate entities (stored in SQLite somewhere)
ftm map landlord_tracker_mappings.yml | ftm store write -d landlord_tracker

# stream the aggregated entities into Aleph
ftm store iterate -d landlord_tracker | aleph load-entities landlord_tracker

# delete the stored entities in SQLite
ftm store delete -d landlord_tracker

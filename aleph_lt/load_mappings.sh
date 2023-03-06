#!/bin/bash

cd /landlord-tracker/aleph_lt
ftm map landlord_tracker_mappings.yml | aleph load-entities landlord_tracker

# landlord-tracker

Hi! to set up the landlord-tracker postgres you should just have to run install_tracker.sh in this directory. However, this may not work on the first try so if you enconter any bugs during the process, please message thomas.da.paine on slack or in the signal to fix the problems you encounter. Thanks!

* Note, for right now it is necessary to change the datanames in the data file, just look at ./landlord-tracker/engles/config/postgres_db_config.yaml at the table_keys:raw_path variable to see what names directories should be changed to. An injestion script will be written soon!
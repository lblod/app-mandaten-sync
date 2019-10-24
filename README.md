# mandaten sync
This repository contain a solution to manually sync data from digitaal loket to gelinkt notuleren. Specifically, persons and their mandates. 

## Usage 

1. get a recent database dump (backup) for each system and place them in `./data/db-loket/backups`  and `./data/db-gn/backups` respectively. 
2. configure the correct BACKUP_PREFIX in docker-compose.yml
3. run `docker-compose up -d` and wait for the sync service to complete.
4. move the migrations from `./output/` to the gelinkt-notuleren application

NOTE: please verify data before loading in a production system

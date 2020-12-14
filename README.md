Elasticsearch poc:-

This is very basic elasticsearch poc,on running main.py two folders named as archive and malformed created. If any file being added inside
the folder where main.py is running, it reads the file and if data is json then try to push the data on elasticsearch server. The data being 
send is in chunks, if in between this process non-parsable(malformed) json arrives, then all the data will be roll-backed and the file will be
moved to malformed folder. On successfull transaction file will be moved to archive folder.



Usage:

1. Download elasticsearch
2. open cmd goto elasticsearch/bin search elasticsearch.bat and run , wait for starting the server
3. open chrome add Elasticsearch Head for visualizing data.
4. open cmd run py main.py (for main.py clone the git repository)
5. open another cmd run createDataSetForTesting.py (this will create a new file and our eventlistener will get trigger and data will be stored in elasticache)
6. now run createHugeDataSetForRollBackTesting.py.
7. Now refresh chrome ElasticSearch Head data will be added, after some time all the data will be rollbacked as json is not parsable in the file.

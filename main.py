import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler,FileModifiedEvent
import os
from elasticsearch import Elasticsearch,helpers
import ijson
from ijson.common import IncompleteJSONError
import shutil
import uuid
from test import *
import requests
import json


"""
 This function is responsible for rolling back the data if transaction fails due to malformed json
"""
def rollback_data(uniqueId,es,url="http://localhost:9200/yash_tech/_doc/_delete_by_query",index="yash_tech"):
 try:
 #bug bug need some time to process
  time.sleep(1)
  body={"query": {  "match" : {"uniqueId": uniqueId }}}
  body=json.dumps(body)
  response=es.delete_by_query(index=index,body=body)
 except:
  pass

"""
 This function is responsible for adding _index,_doc and uniqueId in a document(once record)
"""
def add_doctype_index(mydata,uniqueId,index="yash_tech",doc="employee"):
  mydata["_index"]=index
  mydata["_doc"]=doc
  mydata["uniqueId"]=uniqueId 


"""
 This function is responsible for firing batch query to store data, to save network overhead.
"""
def store_data_in_elasticsearch(elastic_search,mydata):
   try:
    if len(mydata)==0:return
    response=helpers.bulk(elastic_search,mydata)
   except Exception as e:
    print(e)

"""
 This function is responsible for moving a file to another folder
"""
def move_to_another_folder(filename,folder_name):
 try:
  current=os.path.dirname(os.path.realpath(__file__))
  f=os.path.basename(filename)
  destination=current+os.sep+folder_name+os.sep+f
  shutil.move(filename,destination)
 except Exception as e:
  print(e)



"""
 retrieve_data is responsible for extracting data chunk by chunk from file and calling other functions
"""
def retrieve_data(filename):
  f=open(filename,'r')
  #creating a uniqueId field for rollback option
  uniqueId = str(uuid.uuid4()).strip()
  es=Elasticsearch([{'host':'localhost','port':9200}])
  try:
   #gives iterator hence not reading the file all at once , (json.loads(f.read()) load data all at once hence using ijson)
   jsonObjects=ijson.items(f,'item')
   data=[]
   count=0
   for jsonObject in jsonObjects:
    add_doctype_index(jsonObject,uniqueId)   
    data.append(jsonObject)
    if count!=0 and count%999==0:
     store_data_in_elasticsearch(es,data)
     data=[]
    count+=1
   store_data_in_elasticsearch(es,data)
   f.close()
   move_to_another_folder(filename,"archive")
  except Exception as e:
   if isinstance(e,IncompleteJSONError):
    f.close()
    move_to_another_folder(filename,"malformed")
    rollback_data(uniqueId,es)
  finally:
   f.close()
   

class EventHandler(FileSystemEventHandler):
    def __init__(self):
     #modified trigger working twice so to solve that issue a small trick
     self.flag=False
    def on_created(self,event):
     self.flag=True
     print(event)
    def on_modified(self,event):
     if self.flag==True:
      self.flag=False
      filename=event.src_path
      retrieve_data(filename)           
     self.flag=False

"""
 This function is responsible for creating two folders named as archive and malformed if folder does not exists
"""
def create_folder(path):
 archive_folder=path+os.sep+"archive"
 malformed_folder=path+os.sep+"malformed"
 if os.path.isdir(archive_folder)==False:
  os.mkdir(archive_folder)
 if os.path.isdir(malformed_folder)==False:
  os.mkdir(malformed_folder)

     

if __name__ == "__main__":
    path =os.path.dirname(os.path.realpath(__file__))
    create_folder(path)
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
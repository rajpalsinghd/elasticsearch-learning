import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler,FileModifiedEvent
import os
from elasticsearch import Elasticsearch,helpers
import ijson
from ijson.common import IncompleteJSONError
import shutil
import uuid




def rollback_data(uniqueId):
 print(uniqueId)

def add_doctype_index(mydata,uniqueId,index="yash_tech",doc="employee"):
  mydata["_index"]=index
  mydata["_doc"]=doc
  mydata["uniqueId"]=uniqueId 

def store_data_in_elasticsearch(elastic_search,mydata):
   try:
    if len(mydata)==0:return
    response=helpers.bulk(elastic_search,mydata)
   except Exception as e:
    print(e)

def move_to_another_folder(filename,folder_name):
 try:
  current=os.path.dirname(os.path.realpath(__file__))
  f=os.path.basename(filename)
  destination=current+os.sep+folder_name+os.sep+f
  shutil.move(filename,destination)
 except Exception as e:
  print(e)

def retrieve_data(filename):
  f=open(filename,'r')
  uniqueId = uuid.uuid4() 
  es=Elasticsearch([{'host':'localhost','port':9200}])
  try:
   #gives iterator hence not reading the file all at once
   jsonObjects=ijson.items(f,'item')
   data=[]
   for count,jsonObject in enumerate(jsonObjects):
    add_doctype_index(jsonObject,uniqueId)   
    data.append(jsonObject)
    if count!=0 and count%999==0:
     store_data_in_elasticsearch(es,data)
     data=[]
   store_data_in_elasticsearch(es,data)
   f.close()
   move_to_another_folder(filename,"archive")         
  except Exception as e:
   if isinstance(e,IncompleteJSONError):
    f.close()
    move_to_another_folder(filename,"malformed")
    rollback_data(uniqueId)
  finally:
   f.close()
   es.transport.close()
 



class EventHandler(FileSystemEventHandler):
    def __init__(self):
     #modified trigger working twice so to solve that issue a small trick
     self.flag=False
    def on_created(self,event):
     self.flag=True
    def on_modified(self,event):
     if self.flag==True:
      self.flag=False
      filename=event.src_path
      retrieve_data(filename)           
     self.flag=False
     

if __name__ == "__main__":
    path =os.path.dirname(os.path.realpath(__file__))
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
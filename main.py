import glob
import os
import os.path
import datetime, time
import json
from elasticsearch import Elasticsearch,helpers


def add_doctype_index(mydata):
 for m in mydata:
  m["_index"]="yash_tech"
  m["_doc"]="employee"



def store_data_in_elasticsearch(mydata):
 es=Elasticsearch([{'host':'localhost','port':9200}])
 response=helpers.bulk(es,mydata)
 print(response) 


#finding the current directory
dir_path = os.path.dirname(os.path.realpath(__file__))

#listing all the json files inside current directory
listFiles = glob.glob(dir_path+'/*.json')

#finding the last file creation time(extracting from logs)
last_time_stamp=0
f=open('timestamp.logs','r')
file_info=dict()
try:
 file_info=json.loads(f.read())
 f.close()
 last_time_stamp=file_info['time_stamp']
except:
 f.close()

#finding all the files being dumped after last dumping process by server.
to_read_files=[]
new_time_stamp=last_time_stamp
for file in listFiles:
 time_stamp=os.path.getmtime(file)
 if time_stamp>last_time_stamp:
  to_read_files.append(file)
  if new_time_stamp<time_stamp:new_time_stamp=time_stamp

print(to_read_files)
#read all the files
try:
 for current_file in to_read_files: 
  with open(current_file, 'r') as f:
   mydata = json.loads(f.read())
   add_doctype_index(mydata)
   store_data_in_elasticsearch(mydata)
except Exception as e:
 print(e)

#updating the file logs
file_info['time_stamp']=new_time_stamp
f=open('timestamp.logs','w')
f.write(json.dumps(file_info))
f.close()





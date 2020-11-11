import json

a=[
{
"name":"rajpal"+str(number),
"age":number*1
}
for number in range(2002)]


f=open("temp.data","w")
f.write(json.dumps(a))
f.close()

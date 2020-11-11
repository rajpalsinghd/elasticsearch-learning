import json
a=[
{
"name":"rajpal"+str(number),
"age":number*1
}
for number in range(5000)]
f=open("temp1.data","w")
p=f.write(json.dumps(a))
f.seek(p-1)
f.write(",{'name':'raj','age':34}]")
f.close()
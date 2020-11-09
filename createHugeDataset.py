import json

a=[
{
"name":"rajpal"+str(number),
"age":0*number
}
for number in range(1000)
]

f=open('data1.json','w')
f.write(json.dumps(a))
f.close()


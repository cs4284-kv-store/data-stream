import redis
import requests
import re
import datetime
import sys

if len(sys.argv) < 2:
	print("ERROR")
	sys.exit()
sensor_name = sys.argv[1]
r = redis.Redis(host='localhost', port=6379, db=0)
time_stamp = str(datetime.datetime.utcnow() - datetime.timedelta(seconds=3))
url = "http://localhost:8080/" + sensor_name
regex = time_stamp[0:len(time_stamp) - 7] + r'.*\..*\,.*\..*'
reg = re.compile(regex)
data = r.lrange(sensor_name, 0, -1)
times = filter(reg.match, data)
values = []
for item in times:
	values.append(item.split(',')[1])
values = list(map(float, values))
if len(values) != 0:
	requests.post(
			url, 
			data={
				'avg': sum(values)/len(values), 
				'id': sensor_name, 
				'max': max(values), 
				'min': min(values), 
				'updated': time_stamp[0:len(time_stamp) - 7]
			}
		)

import redis
import requests
from datetime import datetime

r = redis.Redis(host='localhost', port=6379, db=0)
sensor_list = r.lrange('sensor_list', 0, r.llen('sensor_list'))

time_stamp = str(datetime.utcnow())

for sensor_name in sensor_list:
	url = "http://localhost:8080/" + sensor_name
	key = sensor_name + ':' + time_stamp
	data = []
	print(list(r.scan_iter(match = key[0:len(key) - 28] + '*')))
	#for item in r.scan_iter(match = key[0:len(key) - 7] + '*'):
	#	# data.append(r.get(item))
	#	print(item)
	data = map(float, data)
	if len(data) != 0:
		s = requests.post(
				url, 
				data={
					'avg': sum(data)/len(data), 
					'id': sensor_name, 
					'max': max(data), 
					'min': min(data), 
					'updated': time_stamp[0:len(time_stamp) - 7]
				}
			)
		print(s)

print("done")

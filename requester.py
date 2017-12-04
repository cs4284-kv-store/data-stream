import redis
import requests
import re
import datetime
import time

r = redis.Redis(host='localhost', port=6379, db=0)
sensor_list = r.lrange('sensor_list', 0, r.llen('sensor_list'))

time_stamp = str(datetime.datetime.utcnow() - datetime.timedelta(seconds=2))

while 1:
	t0 = time.time()
	for sensor_name in sensor_list:
		url = "http://localhost:8080/" + sensor_name
		regex = time_stamp[0:len(time_stamp) - 8] + r'.*\..*\,.*\..*'
		reg = re.compile(regex)
		data = r.lrange(sensor_name, 0, -1)
		times = filter(reg.match, data)
		values = []
		for item in times:
			values.append(item.split(',')[1])
		values = list(map(float, values))
		if len(values) != 0:
			s = requests.post(
					url, 
					data={
						'avg': sum(values)/len(values), 
						'id': sensor_name, 
						'max': max(values), 
						'min': min(values), 
						'updated': time_stamp[0:len(time_stamp) - 7]
					}
				)
			print(s)
	t1 = time.time()
	wait = t1 - t0
	if (1 - wait) > 0:
		time.sleep(1 - wait)

import redis
import requests
import re
import datetime
import time

r = redis.Redis(host='10.142.0.2', port=6379, db=0, password='correcthorsebatterystaple')
sensor_list = r.lrange('sensor_list', 0, r.llen('sensor_list'))

sess = requests.session()

while 1:
    time_stamp = str(datetime.datetime.utcnow() - datetime.timedelta(seconds=1))
    t0 = time.time()
    for sensor_name in sensor_list:
        url = "http://10.142.0.4/" + sensor_name
        regex = time_stamp[0:len(time_stamp) - 7] + r'.*\..*\,.*\..*'
        reg = re.compile(regex)
        data = r.lrange(sensor_name, 0, -1)
        times = filter(reg.match, data)
        values = [float(item.split(',')[1]) for item in times]

        print(len(values))

        if len(values) != 0:
            data={
                'avg': sum(values)/len(values), 
                'id': sensor_name, 
                'max': max(values), 
                'min': min(values), 
                'updated': time_stamp[0:len(time_stamp) - 7]
                }
            s = sess.post(
                    url, 
                    json=data
                    )
    t1 = time.time()
    wait = t1 - t0
    print(1 - wait)
    if (1 - wait) > 0:
        time.sleep(1 - wait)

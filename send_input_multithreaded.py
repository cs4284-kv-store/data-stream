#!/usr/bin/python
import csv  
import redis
import sys
import threading 
import time 
from datetime import datetime


#print(datetime.utcnow())
#print(len("3C-24:%s" % datetime.utcnow()))

usage = "Error: incorrect usage please use the following flags in any order \n\
            -l if you want the script to run forever \n\
            -p [port number]\n\
            -h [hostname]\n\
            -pass [password]\n\
            -f [path to datafile]\n\
            -n [number of commands sent to pipeline before execution (affects performance)]\n\
            example: python send_input_multithreaded.py -l -p 6379 -pass guest -h localhost -f datafile.csv -n 25000"



if(len(sys.argv) < 2):
    print(usage)
    sys.exit()

hostname="nothing"
portnumber="nothing"
passwd=""
persistent = False
cmds_per_pipeline = -1
data_path = "not_a_file"


for arg in range(len(sys.argv)):
    if sys.argv[arg] == "-p": #port
        portnumber = sys.argv[arg + 1]
    if sys.argv[arg] == "-l": # is persistent
        persistent = True
    if sys.argv[arg] == "-h": #hostname
        hostname = sys.argv[arg + 1]
    if sys.argv[arg] == "-pass":
        passwd = sys.argv[arg + 1]
    if sys.argv[arg] == "-n": # number of commands givne to each pipeline before it is sent
        cmds_per_pipeline = int(sys.argv[arg + 1])
    if sys.argv[arg] == "-f": # datafile
        data_path = sys.argv[arg + 1]


print("parsed args are : port number = %s, is persistnet = %s hostname = %s cmds Per pipe = %d data_path = %s passwd = %s " %(portnumber, persistent, hostname, cmds_per_pipeline, data_path, passwd))

if cmds_per_pipeline < 1 or hostname == "nothing" or portnumber == "nothing" or data_path == "not_a_file":
    print(usage)
    sys.exit()


database = redis.Redis(host=hostname, port=portnumber, password=passwd)
database.flushall()

sensor_list = "sensor_list"


def send_thread_row(row, cmds_per_pipe):
        
        keybase = "%s" %(row[0])
        pipe = database.pipeline()
        database.lpush(sensor_list, row[0])
        
        print(row[0])
        #print(len(row))
        #round_number = 0.0 # if peristent we add the number of times through the data set to each float value
        data_point = 11
        
        pipe_number = 0

        while True: ##acting as a do while loop for the case when we want the stream to be persistent
            while data_point < (len(row) - 11):
                start_time = time.time()
                if data_point + cmds_per_pipe > len(row):
                    next_pipe_length =  len(row) - data_point - 1
                else :
                    next_pipe_length = int(cmds_per_pipe)
                for i in range(next_pipe_length):
                    value = float(row[data_point + i]) 
                    #print("row %s %s %f" %(row[0],datetime.utcnow(), value))
                    #pipe.set("%s:%s" % (keybase, datetime.utcnow()), "%f" % value)
                    pipe.lpush("%s" % keybase, "%s,%f" % (datetime.utcnow(),value))
                pipe.ltrim("%s" % keybase, 0, cmds_per_pipe * 55)
                pipe.execute()
                sleep_for = start_time + 1 - time.time()
                time.sleep(sleep_for)
                data_point += (int(cmds_per_pipe) + 1)
                pipe_number += 1
            data_point = 11

            if not persistent: ## if not persistnet break out of the loop after the first run
                break


#print(time.clock())

thread_list = []
#file_contents = 
def main(cmds_per_pipe):
    data = open(data_path, 'r')
    reader = csv.reader(data, delimiter=',')
    first = True
    for row in reader:
        if first == True:
            #print(row)
            first = False
            continue
        ##start thread for each row to mimic the sensors 
        t = threading.Thread(target=send_thread_row, args=(row, cmds_per_pipe))
        thread_list.append(t)
    start = time.clock()
    #print("starting with %d threads" % len(thread_list))
    for thread in thread_list:
        thread.start()
    
    #time.sleep(10)
    #print("length of the sensor list %d" % database.llen(sensor_list))
    
    #print("waiting")
    for thread in thread_list:
        thread.join()
        #print("joining")
    end = time.clock()
    func_time = end - start
    print("done in time %f" % func_time)

    

main(cmds_per_pipeline)

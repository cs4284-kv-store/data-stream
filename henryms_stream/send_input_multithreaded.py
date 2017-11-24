#!/usr/bin/python
import csv  
import redis
import sys
import threading 
import time 




if(len(sys.argv) < 2):
    print("error: Usage python send_input.py host port password file.csv [cmds_per_pipeline] -p[continuous]\n \
            \t ie: python send_imput.py localhost 6379 guest 30000 -p")
    sys.exit()

hostname=sys.argv[1]
portnumber=sys.argv[2]
passwd=sys.argv[3]
database = redis.Redis(host=hostname, port=portnumber, password=passwd)
persistent = False
if len(sys.argv) >= 7 and sys.argv[6] == "-p":
    persistent = True

def send_thread_row(row, cmds_per_pipe):
        ID=0
        keybase = "%s" %(row[0])
        pipe = database.pipeline()
        #print(row[0])
        #print(len(row))
        data_point = 11
        pipe_number = 0
        while data_point < (len(row) - 11):
            if data_point + cmds_per_pipe > len(row):
                next_pipe_length = data_point - len(row) - 1
            else :
                next_pipe_length = int(cmds_per_pipe)
            for i in range(next_pipe_length):
                ID += 1
                value = float(row[data_point + i])
                #print(value)
                pipe.set("%s:%d:%d" % (keybase, pipe_number,ID), "%f" % value)
            pipe.execute()
            data_point += (int(cmds_per_pipe) + 1)
            pipe_number += 1

            
thread_list = []
#file_contents = 
def main(cmds_per_pipe):
    data = open(sys.argv[4], 'r')
    reader = csv.reader(data, delimiter=',')
    date = "X/X/X"
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
    print("starting")
    for thread in thread_list:
        thread.start()
    
    
    print("waiting")
    for thread in thread_list:
        thread.join()
    end = time.clock()
    func_time = end - start
    print("done in time %f" % func_time)
        
if persistent == True:
    while True:
        print("start")
        main(int(sys.argv[5]))
        print("end")
else :
    #print("nothing here")
    main(int(sys.argv[5]))

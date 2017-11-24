#!/usr/bin/python
import csv  
import redis
import sys
import threading 
import time 




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
portnumber="nohing"
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

if cmds_per_pipeline < 1 or passwd == "" or hostname == "nothing" or portnumber == "nothing" or data_path == "not_a_file":
    print(usage)
    sys.exit()


database = redis.Redis(host=hostname, port=portnumber, password=passwd)

def send_thread_row(row, cmds_per_pipe):
        ID=0
        keybase = "%s" %(row[0])
        pipe = database.pipeline()
        print(row[0])
        #print(len(row))
        round_number = 0.0 # if peristent we add the number of times through the data set to each float value
        data_point = 11
        pipe_number = 0
        while True: ##acting as a do while loop for the case when we want the stream to be persistent
            if round_number == 0.004:
                print("running in persistnet mode")
            while data_point < (len(row) - 11):
                if data_point + cmds_per_pipe > len(row):
                    next_pipe_length = data_point - len(row) - 1
                else :
                    next_pipe_length = int(cmds_per_pipe)
                for i in range(next_pipe_length):
                   ID += 1
                   value = float(row[data_point + i]) + round_number
                   print("row %s %f" %(row[0],value))
                   pipe.set("%s:%d:%d" % (keybase, pipe_number,ID), "%f" % value)
                pipe.execute()
                data_point += (int(cmds_per_pipe) + 1)
                pipe_number += 1
            round_number += 0.001
            data_point = 11
            if not persitent: ## if not persistnet break out of the loop after the first run
                break

            
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
    print("starting")
    for thread in thread_list:
        thread.start()
    
    
    print("waiting")
    for thread in thread_list:
        thread.join()
        print("joining")
    end = time.clock()
    func_time = end - start
    print("done in time %f" % func_time)
        
if persistent == True:
    while True:
        print("start")
        main(cmds_per_pipeline)
        print("end")
else :
    #print("nothing here")
    main(cmds_per_pipeline)

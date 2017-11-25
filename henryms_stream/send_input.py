#!/usr/bin/python
import csv  
import redis
import sys



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
        ID=0
        keybase = "%s" %(row[0])
        pipe = database.pipeline()
        test_pipe = database.pipeline()
        #print(row[0])
        #print(len(row))
        data_point = 11
        while data_point < (len(row) - 11):
            if data_point + cmds_per_pipe > len(row):
                next_pipe_length = data_point - len(row) - 1
            else :
                next_pipe_length = int(cmds_per_pipe)
            for i in range(next_pipe_length):
                ID += 1
                value = float(row[data_point + i])
                #print(value)
                pipe.set("%s:%d:%d" % (keybase, data_point, ID), "%f" % value)
            pipe.execute()
            data_point += (int(cmds_per_pipe) + 1)

if persistent == True:
    while True:
        print("start")
        main(cmds_per_pipeline)
        print("end")
else :
    main(cmds_per_pipeline)
